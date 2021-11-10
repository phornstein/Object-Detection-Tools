# -*- coding: utf-8 -*-

from datetime import time
import os
import sys
import arcpy
import pandas as pd
from arcgis.features import GeoAccessor,GeoSeriesAccessor
from arcgis.geometry import find_transformation, Geometry, SpatialReference
import multiprocessing
from multiprocessing.pool import ThreadPool

from arcdetect import *

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Object Detection Tools"
        self.alias = "Object Detection Tools"

        # List of tool classes associated with this toolbox
        self.tools = [SpaceTimeCorrelation,AttributeImageDetections]


class AttributeImageDetections(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Add Image Chip to Detection"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        detectionFC = arcpy.Parameter(
            displayName="Detection Features:",
            name="detectionFC",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        sourceImage = arcpy.Parameter(
            displayName="Source Image:",
            name="sourceImage",
            datatype="GPRasterLayer",
            parameterType="Required",
            direction="Input"
        )
        storeAsBlob = arcpy.Parameter(
            displayName="Store Image as BLOB?",
            name="storeAsBlob",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input"
        )
        detectionFC.filter.list = ['Polygon']
        params = [detectionFC,sourceImage,storeAsBlob]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        detections = parameters[0].valueAsText
        img = parameters[1].valueAsText
        processBlob = parameters[2].value
        
        if processBlob is True:
            processBlobImages(detections,img)
        else:
            processImagesAsAttachments(detections,img)

class SpaceTimeCorrelation(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Space Time Correlation"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        detectionFC = arcpy.Parameter(
            displayName="Detection Features:",
            name="detectionFC",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        detectionTimeField = arcpy.Parameter(
            displayName="Detection Time Field:",
            name="detectionTimeField",
            datatype="Field",
            parameterType="Required",
            direction="Input"
        )
        tracksFC = arcpy.Parameter(
            displayName="Tracks Features:",
            name="tracksFC",
            datatype="GPFeatureLayer",
            parameterType="Required",
            direction="Input"
        )
        trackIDField = arcpy.Parameter(
            displayName="Track ID Field:",
            name="trackIDField",
            datatype="Field",
            parameterType="Required",
            direction="Input"
        )
        trackTimeField = arcpy.Parameter(
            displayName="Track Time Field:",
            name="trackTimeField",
            datatype="Field",
            parameterType="Required",
            direction="Input"
        )
        outputFeatures = arcpy.Parameter(
            displayName="Output Features:",
            name="outputFeatures",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Output"
        )
        distanceTolerance = arcpy.Parameter(
            displayName="Distance Tolerance (m):",
            name="distanceTolerance",
            datatype="GPLong",
            parameterType="Required",
            direction="Input"
        )

        detectionFC.filter.list = ['Polygon']
        detectionTimeField.parameterDependencies = [detectionFC.name]
        detectionTimeField.filter.list = ['Date']
        tracksFC.filter.list = ['Point']
        trackIDField.parameterDependencies = [tracksFC.name]
        trackTimeField.parameterDependencies = [tracksFC.name]
        trackTimeField.filter.list = ['Date']
        distanceTolerance.value = "800"

        params = [detectionFC,detectionTimeField,tracksFC,trackIDField,trackTimeField,outputFeatures,distanceTolerance]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""
        detectionFC = parameters[0].valueAsText
        detectionTimeField = parameters[1].valueAsText
        trackFC = parameters[2].valueAsText
        trackIDField = parameters[3].valueAsText
        trackTimeField = parameters[4].valueAsText
        output = parameters[5].valueAsText
        distanceTolerance = parameters[6].value

        detectionIDField = arcpy.Describe(detectionFC).OIDFieldName

        detection_candidates = spaceTimeMatch(detectionFC,
                                detectionIDField,
                                detectionTimeField,
                                trackFC,
                                trackIDField,
                                trackTimeField,
                                distanceTolerance)
        
        if len(detection_candidates):
            mem_fc = r'memory/temp_matches'
            arcpy.SetProgressorLabel("Processing Candidate Matches...")
            arcpy.AddMessage("Found {} candidate matches...".format(len(detection_candidates)))
            new_sdf = pd.concat(detection_candidates)
            new_sdf.spatial.to_featureclass(mem_fc)

            arcpy.AddMessage("Joining tracks to detections...")
            arcpy.analysis.SpatialJoin(detectionFC, 
                                        mem_fc, 
                                        output,
                                        "JOIN_ONE_TO_ONE", 
                                        "KEEP_ALL", 
                                        None,
                                        "INTERSECT", 
                                        None, 
                                        '')
            return output

        else:
            arcpy.AddError("No matches found...")
            return None
