# -*- coding: utf-8 -*-
r""""""
__all__ = ['AttributeImageDetections', 'SpaceTimeCorrelation']
__alias__ = 'Object Detection Tools'
from arcpy.geoprocessing._base import gptooldoc, gp, gp_fixargs
from arcpy.arcobjects.arcobjectconversion import convertArcObjectToPythonObject

# Tools
@gptooldoc('AttributeImageDetections', None)
def AttributeImageDetections(detectionFC=None, sourceImage=None, storeAsBlob=None):
    """AttributeImageDetections(detectionFC, sourceImage, {storeAsBlob})

        Add Image Chip to Detection is used to automatically create image
        chips of detections and append the chips into the detection feature
        class' attributes.

     INPUTS:
      detectionFC (Feature Layer):
          The polygon feature class containing bounding boxes or outlines of
          detections from an image.
      sourceImage (Raster Layer):
          The image from which the detection feature class was created. The
          image chips will be created from this image.
      storeAsBlob {Boolean}:
          Optional parameters. If selected the image chips will be added to
          the detection feature class ONLY as a BLOB field in the attribute
          table."""
    from arcpy.geoprocessing._base import gp, gp_fixargs
    from arcpy.arcobjects.arcobjectconversion import convertArcObjectToPythonObject
    try:
        retval = convertArcObjectToPythonObject(gp.AttributeImageDetections(*gp_fixargs((detectionFC, sourceImage, storeAsBlob), True)))
        return retval
    except Exception as e:
        raise e

@gptooldoc('SpaceTimeCorrelation', None)
def SpaceTimeCorrelation(detectionFC=None, detectionTimeField=None, tracksFC=None, trackIDField=None, trackTimeField=None, outputFeatures=None, distanceTolerance=None):
    """SpaceTimeCorrelation(detectionFC, detectionTimeField, tracksFC, trackIDField, trackTimeField, outputFeatures, distanceTolerance)

        Space Time Correlation will take an input feature class of
        detections and feature class of track data and attempt to correlate
        tracks to detections in both space and time.

     INPUTS:
      detectionFC (Feature Layer):
          The polygon feature class containing bounding boxes or outlines of
          detections from an image.
      detectionTimeField (Field):
          The field name of the detection feature class field containing the
          time information of the detections.
      tracksFC (Feature Layer):
          The point feature class containing the track data to be matched to
          the detections. <SPAN />
      trackIDField (Field):
          The field name of the track feature class field containing the
          unique id of the track points. <SPAN />  <SPAN />
      trackTimeField (Field):
          The field name of the track feature class field containing the time
          information of the track points. <SPAN />
      distanceTolerance (Long):
          The search distance for spatially proximity in Meters. Tracks that
          pass within this threshold distance to the detections will also be
          evaluated for temporal matching. <SPAN />

     OUTPUTS:
      outputFeatures (Feature Class):
          The name of the output polygon feature class. The output will match
          the input detections but will also include spatial join information of
          tracks that can correlated to detections in space and time. <SPAN />"""
    from arcpy.geoprocessing._base import gp, gp_fixargs
    from arcpy.arcobjects.arcobjectconversion import convertArcObjectToPythonObject
    try:
        retval = convertArcObjectToPythonObject(gp.SpaceTimeCorrelation(*gp_fixargs((detectionFC, detectionTimeField, tracksFC, trackIDField, trackTimeField, outputFeatures, distanceTolerance), True)))
        return retval
    except Exception as e:
        raise e


# End of generated toolbox code
del gptooldoc, gp, gp_fixargs, convertArcObjectToPythonObject
