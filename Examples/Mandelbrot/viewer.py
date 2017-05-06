#!/usr/bin/python
import pyglet
from pyglet.gl import *
import argparse

def main():
    parser = argparse.ArgumentParser(description = "View a generated Mandelbrot image.")
    parser.add_argument('infoFileName', metavar='f', type=str,
                        help = "The name of the info file.")
    parser.add_argument('--windowHeight', metavar='height', type=int,
                        help = "The height of the viewing window in pixels",
                        default = 800)
    args = parser.parse_args()
    
    window = ViewWindow(args.infoFileName, args.windowHeight)
    pyglet.app.run()

class ViewWindow(pyglet.window.Window):
    def __init__(self, fname, height):
        super(ViewWindow, self).__init__(width=height, height=height, caption="Mandelbrot Viewer")
        self.images = []

        #Extract data from info file.
        dataFile = open(fname)
        data = eval("".join(dataFile.readlines()))
        dataFile.close()
        self.numImages = data["numImages"]
        imageNames = data["imageNames"]
        #The nestingFactor is the scale factor between images.
        # Example: a factor of 3 means the later of two consecutive images
        #         is a higher res version of the middle 1/3rd of the outer.
        self.nestingFactor = data["nestingFactor"] 

        #Load images
        for name in imageNames:
            self.images.append(pyglet.image.load(name))

        #Set aspect ratio from first image.
        imw = self.images[0].width
        imh = self.images[0].height
        self.width = int(self.height*(float(imw)/imh))

        #Zoom so first image is snug with window
        self.borderScale = float(self.height)/imh

        #Values involved with zooming
        self.imgNumber = 0
        self.scaleFactor = 1.0
        self.zoomSpeed = 1.1 #Multiplier for zooming

        #Re-anchor the images at their center
        for i in range(self.numImages):
            self.images[i].anchor_x = self.images[i].width//2
            self.images[i].anchor_y = self.images[i].height//2

    def on_mouse_scroll(self, x,y,scroll_x, scroll_y):
        if scroll_y > 0:
            #Zoom in
            self.scaleFactor *= self.zoomSpeed

            #If zoomed in to the nesting factor change to the next image,
            # and zoom back out to keep visual continuity.
            if self.scaleFactor > self.nestingFactor and self.imgNumber < self.numImages-1:
                self.imgNumber += 1
                self.scaleFactor = 1.0
        elif scroll_y < 0:
            #Zoom out
            self.scaleFactor /= self.zoomSpeed

            #Same as above but zooming out.
            if self.scaleFactor < 1.0 and self.imgNumber > 0:
                self.imgNumber -= 1
                self.scaleFactor = self.nestingFactor

    def on_draw(self,):
        self.clear()
        glPushMatrix()

        #Translate so the origin is centered
        glTranslatef(self.width//2, self.height//2, 0)
        #Apply zoom to fit image in window
        glScalef(self.borderScale, self.borderScale, 1)
        #Zoom
        glScalef(self.scaleFactor, self.scaleFactor, 1)
        #Draw the current image.
        self.images[self.imgNumber].blit(0,0)

        glPopMatrix()
if __name__ == '__main__':
    main()