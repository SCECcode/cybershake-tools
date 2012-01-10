#!/usr/bin/env python

# Imports
import sys
import os
import pwd
import time
from Config import *


class Curve:
    curve_id = None
    im_id = None
    im_measure = None
    im_value = None
    im_units = None

    def __init__(self):
        self.curve_id = None
        self.im_id = None
        self.im_measure = None
        self.im_value = None
        self.im_units = None

    def copy(self, obj):
        self.curve_id = obj.curve_id
        self.im_id = obj.im_id
        self.im_measure = obj.im_measure
        self.im_value = obj.im_value
        self.im_units = obj.im_units

    #@staticmethod
    def formatHeader(self):
        headers = ["Curve ID", "IM Measure", "IM Value", "IM Units",]
        return headers

    def formatData(self):
        data = [str(self.curve_id), \
                    str(self.im_measure), \
                    str(self.im_value), \
                    str(self.im_units),]
        return data

    def getCurveID(self):
        return self.curve_id

    def setCurveID(self, curve_id):
        if (curve_id == None):
            self.curve_id = curve_id
        else:
            self.curve_id = int(curve_id)

    def getIMID(self):
        return self.im_id

    def setIMID(self, im_id):
        if (im_id == None):
            self.im_id = im_id
        else:
            self.im_id = int(im_id)

    def getIMMeasure(self):
        return self.im_measure

    def setIMMeasure(self, im_measure):
        if (im_measure == None):
            self.im_measure = im_measure
        else:
            self.im_measure = str(im_measure)

    def getIMValue(self):
        return self.im_value

    def setIMValue(self, im_value):
        if (im_value == None):
            self.im_value = im_value
        else:
            self.im_value = float(im_value)

    def getIMUnits(self):
        return self.im_units

    def setIMUnits(self, im_units):
        if (im_units == None):
            self.im_units = im_units
        else:
            self.im_units = str(im_units)
