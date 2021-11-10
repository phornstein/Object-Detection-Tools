from datetime import time
import os
import sys
import arcpy
import pandas as pd
from arcgis.features import GeoAccessor,GeoSeriesAccessor
from arcgis.geometry import find_transformation, Geometry, SpatialReference
import multiprocessing
from multiprocessing.pool import ThreadPool

def spaceTimeMatch(detectionFeatureClass, detectionIDField, detectionTimeField, tracksFeatureClass, trackIDField, trackTimeField, distanceTolerance=800):
    """Conducts a space time match of detections and associated track datas.
    
    Parameters
    ----------
    detectionFeatureClass : str
        path to polygon featureclass of detections
        
    detectionIDField : str
        unique id field of detections
        
    detectionTimeField : str
        field containing time of detection
        
    tracksFeatureClass : str
        path to point featureclass of points representing tracks in the area of interst
        
    trackIDField : str
        unique id field of tracks
        
    trackTimeField : str
        field containing time of each track point
        
    distanceTolerance : int
        search distance for tracks to intersect detections

    Returns
    -------
    list of spatially enabled dataframes of tracks that match a detection in space and time. The 
    unique ID of the matched detection is stored in the column "detection_oid"
    """
    sdf = pd.DataFrame.spatial.from_featureclass(tracksFeatureClass) #get sdf of track points
    sdf = project_as(sdf,3857)

    unique_tracks = _getUniqueTrackDFs(sdf,trackIDField)
    del sdf

    aoi_df = pd.DataFrame.spatial.from_featureclass(detectionFeatureClass)
    aoi_df = project_as(aoi_df,3857)
    aoi_df['SHAPE'] = aoi_df['SHAPE'].geom.buffer(distanceTolerance) #buffer all polygons to the search distance

    detection_candidates = []
    def worker(sdf):
        '''Worker function to distribute space time matching'''
        sdf.drop_duplicates(subset=[trackTimeField],inplace=True) #sort df by time to make sure tracks are processed in logical order
        sdf.sort_values(by=[trackTimeField],inplace=True)
        sdf = sdf.reset_index(drop=True)
        num_points = len(sdf.index)

        for ind,row in aoi_df.iterrows():
            timestamp = row[detectionTimeField]
            aoi = arcpy.AsShape(row['SHAPE'],True)

            current_point = arcpy.AsShape(sdf['SHAPE'].iloc[0],True)
            next_point = None
            
            for i in range(0,num_points-1):             
                next_point = arcpy.AsShape(sdf['SHAPE'].iloc[i+1],True)
                line = arcpy.Polyline(arcpy.Array(items=[current_point.firstPoint,next_point.firstPoint]),3857)
                if aoi.disjoint(line) is False:
                    if (sdf[trackTimeField].iloc[i] < timestamp) and (sdf[trackTimeField].iloc[i+1] > timestamp):
                        sdf['detection_oid'] = row[detectionIDField]
                        detection_candidates.append(sdf)
                        return True
            
    _executeMultiprocessTask(worker,unique_tracks,'Evaluating track ')

    return detection_candidates

def _getUniqueTrackDFs(sdf, trackIDField):
    '''
    Wrapper to split a pandas df into a list of dfs by unique id

    Parameters
    ----------
    sdf : pandas dataframe
        dataframe to be split

    trackIDField : str
        column name containing the unique id

    Returns
    -------
    list of pandas dataframes with one track per df
    '''
    unique_tracks = [] #gets list of tracks
    for uid, df_uid in sdf.groupby(trackIDField):
        unique_tracks.append(df_uid)
    return unique_tracks

def _executeMultiprocessTask(workerFunction,uniqueData,progressorLabel):
    '''
    Wrapper for handling multiprocessing tasks primarily across a dataframe.

    Parameters
    ----------
    workerFunction : Function
        A static function to be executed in a distributed fashion. This function
        should take 1 input parameter.

    uniqueData : [variable]
        Unique data is a list of the data to be passed to the worker function during
        processing.

    progressorLabel : str
        The progressor label to be shown on the ArcGIS Pro GP tool. Note: include trailing
        white space in this string if desired.
    '''
    multiprocessing.set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe'))
    numTracks = len(uniqueData)
    pool = ThreadPool(processes=numTracks)
    for indx,rslt in enumerate(pool.imap_unordered(workerFunction,uniqueData)):
        arcpy.SetProgressorPosition(int(indx/numTracks*100))
        arcpy.SetProgressorLabel('{} {}/{}...'.format(progressorLabel,indx+1,numTracks))
    pool.close()

def project_as(input_dataframe: pd.DataFrame, output_spatial_reference: int = 4326,
            input_spatial_reference: int = None,
            transformation_name: str = None) -> pd.DataFrame:
    """
    Project input Spatially Enabled Dataframe to a desired output spatial reference, applying a
        transformation if needed due to the geographic coordinate system changing.
    Args:
        input_dataframe: Valid Spatially Enabled DataFrame
        output_spatial_reference: Optional - Desired output Spatial Reference. Default is
            4326 (WGS84).
        input_spatial_reference: Optional - Only necessary if the Spatial Reference is not
            properly defined for the input data geometry.
        transformation_name: Optional - Transformation name to be used, if needed, to
            convert between spatial references. If not explicitly provided, this will be
            inferred based on the spatial reference of the input data and desired output
            spatial reference.
    Returns: Spatially Enabled DataFrame in the desired output spatial reference.
    """
    # ensure the geometry is set
    geom_col_lst = [c for c in input_dataframe.columns if input_dataframe[c].dtype.name.lower() == 'geometry']
    assert len(geom_col_lst) > 0, 'The DataFrame does not appear to have a geometry column defined. This can be ' \
                                'accomplished using the "input_dataframe.spatial.set_geometry" method.'

    # save the geometry column to a variable
    geom_col = geom_col_lst[0]

    # ensure the input spatially enabled dataframe validates
    # assert input_dataframe.spatial.validate(), 'The DataFrame does not appear to be valid.'

    # if a spatial reference is set for the dataframe, just use it
    if input_dataframe.spatial.sr is not None:
        in_sr = input_dataframe.spatial.sr

    # if a spatial reference is explicitly provided, but the data does not have one set, use the one provided
    elif input_spatial_reference is not None:

        # check the input
        assert isinstance(input_spatial_reference, int) or isinstance(input_spatial_reference, SpatialReference), \
            f'input_spatial_reference must be either an int referencing a wkid or a SpatialReference object, ' \
            f'not {type(input_spatial_reference)}.'

        if isinstance(input_spatial_reference, int):
            in_sr = SpatialReference(input_spatial_reference)
        else:
            in_sr = input_spatial_reference

    # if the spatial reference is not set, common for data coming from geojson, check if values are in lat/lon
    # range, and if so, go with WGS84, as this is likely the case if in this range
    else:

        # get the bounding values for the data
        x_min, y_min, x_max, y_max = input_dataframe.spatial.full_extent

        # check the range of the values, if in longitude and latitude range
        wgs_range = True if (x_min > -181 and y_min > -91 and x_max < 181 and y_max < 91) else False
        assert wgs_range, 'Input data for projection data must have a spatial reference, or one must be provided.'

        # if the values are in range, run with it
        in_sr = SpatialReference(4326)

    # ensure the output spatial reference is a SpatialReference object instance
    if isinstance(output_spatial_reference, SpatialReference):
        out_sr = output_spatial_reference
    else:
        out_sr = SpatialReference(output_spatial_reference)

    # copy the input spatially enabled dataframe since the project function changes the dataframe in place
    out_df = input_dataframe.copy()
    out_df.spatial.set_geometry(geom_col)

    # if arcpy is available, use it to find the transformation
    if transformation_name is None:

        # get any necessary transformations using arcpy, which returns only a list of transformation names
        trns_lst = arcpy.ListTransformations(in_sr.as_arcpy, out_sr.as_arcpy)

    # apply across the geometries using apply since it recognizes the transformation correctly if transformation
    # is necessary and also tries arcpy first, and if not available, rolls back to rest resources elegantly
    if len(trns_lst) or transformation_name is not None:
        trns = transformation_name if transformation_name is not None else trns_lst[0]
        out_df[geom_col] = out_df[geom_col].apply(lambda geom: geom.project_as(out_sr, trns))

    # otherwise, do the same thing using the apply method since the geoaccessor project method is not working reliably
    # and only if necessary if the spatial reference is being changed
    elif in_sr.wkid != out_sr.wkid:
        out_df[geom_col] = out_df[geom_col].apply(lambda geom: geom.project_as(out_sr))

    # ensure the spatial column is set
    if not len([c for c in out_df.columns if out_df[c].dtype.name.lower() == 'geometry']):
        out_df.spatial.set_geometry(geom_col)

    return out_df

def processImagesAsAttachments(detections, image):
    """
    Adds image chips as attachments to the detection featureclass

    Parameters
    ----------
    detections : str
        path to detection featureclass

    image : str
        path to source image of detections
    """
    arcpy.EnableAttachments_management(detections) #enable attachments on detections

    scratchFolder = arcpy.env.scratchFolder #images will be placed in scratch folder
    imgDicts = []
    detectionIDField = arcpy.Describe(detections).OIDFieldName

    with arcpy.da.SearchCursor(detections,[detectionIDField,'SHAPE@']) as cursor:
        for row in cursor:
            img_path = scratchFolder + r"/chip{}.png".format(row[0])
            arcpy.SetProgressorLabel("Processing {}...".format(row[0]))
            with arcpy.EnvManager(extent=row[1].extent):
                arcpy.management.CopyRaster(image, img_path, '', None, "65536", "NONE", "ColormapToRGB", "8_BIT_UNSIGNED", "NONE", "NONE", "PNG", "NONE", "CURRENT_SLICE", "NO_TRANSPOSE")
            imgDicts.append({'id':row[0],
                            'path':img_path})

    memTable = r'memory/matchTable'
    imgTable = pd.DataFrame.from_dict(imgDicts)
    imgTable.spatial.to_table(memTable)

    arcpy.AddAttachments_management(detections,detectionIDField,memTable,'id','path')
    

def processBlobImages(detections,image):
    """
    Adds image chips as blob fields to the detection featureclass

    Parameters
    ----------
    detections : str
        path to detection featureclass

    image : str
        path to source image of detections"""
    temp_img = os.path.join(os.path.dirname(arcpy.env.scratchWorkspace),'temp_img.png')

    #Idea by Andrew King: aking@esri.com
    arcpy.AddField_management(detections,'image',"BLOB")

    with arcpy.da.UpdateCursor(detections,['OID@','image','SHAPE@']) as cursor:
        for row in cursor:
            arcpy.SetProgressorLabel("Processing {}...".format(row[0]))
            arcpy.env.extent = row[2].extent
            arcpy.CopyRaster_management(image,temp_img)
            row[1] = open(temp_img,'rb').read()
            cursor.updateRow(row)
            os.remove(temp_img)
    return detections
