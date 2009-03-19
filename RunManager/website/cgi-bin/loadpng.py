#!/usr/bin/env python

import sys
import cgi


# Enable python stack trace output to HTML
import cgitb; cgitb.enable() 


def init():
    
    return 0


def main():

    form = cgi.FieldStorage() # instantiate only once!

    img = None
    if form.has_key("img") and form["img"].value != "":
        img = form["img"].value

    if ((img != None) and (img.find(".png") != -1)):
        # Dump the .png file to stdout
        print "Content-type: image/png\n"
        print file(r"%s" % (img), "r").read()
    
    return 0


def cleanup():
    return 0


if __name__ == '__main__':
    if (init() != 0):
        sys.exit(1)
    main()
    cleanup()
    sys.exit(0)
