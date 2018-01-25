#!/usr/bin/python
from distribuPy import *
from PIL import Image

def main():
    #Server Info
    ip = "127.0.0.1"
    port = 25565

    #Position to center images at.
    pos =  (-1.2565580000563,   -0.03000038246265)

    #height of zoomed window in the complex plane
    h = 0.83
    aspect_ratio = 16.0/9.0
    w = h*aspect_ratio
    size = (w,h)

    #Resolution of output images.
    ry = 2**10
    rx = int(ry*aspect_ratio)
    resolution = (rx,ry)

    #Division of image for distributing
    tiling = (20,20)

    #All images output get named <baseName><number>.png
    #Info file is named <baseName>Info.json
    baseName = "mandelBrot"

    #Number of output Images to make
    numRepetitions = 10

    server = MandelrotServer(baseName, pos, size, resolution, tiling, numRepetitions)
    server.setup(ip, port)
    server.start_all()
    server.spin()
    server.writeInfoFile()

class MandelrotServer(DistributedTaskManager):
    def __init__(self, baseName, pos, size, resolution, tiling, numRepetitions, tasks_per_job=1):
        self.numReplies = 0
        self.maxRepetitions = numRepetitions

        self.numJobIntegrators = 2
        self.jobsToPop = 3

        self.tiling = tiling
        self.numDivs = tiling[0]*tiling[1]

        self.w, self.h = size
        self.x, self.y = pos
        self.rx, self.ry = resolution
        self.window = Window(pos,size)
        self.genColors()
        self.maxItters = len(self.pallette)-1
        self.info = '"numImages":{}, "imageNames":{}, "nestingFactor":{}'

        self.dx = self.rx/self.tiling[0]
        self.dy = self.ry/self.tiling[1]

        self.img = Image.new('RGB', resolution)
        self.pixels = self.img.load()
        self.baseName = baseName
        #Second arg is a little hacky, gets around defining it for now
        self.outName = "{}{}.png".format(self.baseName,"{}")
        self.imageNames = []

        self.nestingFactor = 10.0/5.0

        DistributedTaskManager.__init__(self, tasks_per_job)

    #Save the current image.
    def saveImage(self, fName):
        self.img.save(fName)
        self.imageNames.append(fName)

    #Writes out a json file for the viewer program.
    def writeInfoFile(self,):
        f = open(self.baseName + "Info.json","w+")
        info = self.info.format(self.maxRepetitions,self.imageNames, self.nestingFactor)
        #The brackets have to be added here or the format function cries.
        f.write('{'+info+'}')

    #Generates n (RGB) tuples linearly between start and end
    def transition(self,start,end,n):
        diff = [ (end[i]-start[i])/float(n) for i in range(len(start))  ]
        colors = []
        for i in range(n+1):
            c = tuple(map(int,map(sum,zip(start,self.scale(diff,i)))))
            colors.append(c)
        return colors

    def scale(self,arr,s):
        #Scales every element of arr by s
        return [ a*s for a in arr ]

    #Defines the color scheme
    def genColors(self):
        #Cyberpunk
        # red = (214,0,255)
        # green=(0,30,255)
        # blue = (0,255,159)

        #Purple
        # red = (102,0,102)
        # green=(128,50,128)
        # blue = (44,0,58)

        #(?"?)
        red = (73,10,61)
        orange = (189,21,80)
        yellow = (233,127,2)
        green = (248,202,0)
        blue = (138,155,15)

        #Blueberries
        # red = (42,4,74)
        # orange = (11,46,89)
        # yellow = (13,103,89)
        # green = (122,179,23)
        # blue = (160,197,95)

        self.pallette = self.transition(red,orange,55)
        self.pallette += self.transition(orange,yellow,55)
        self.pallette += self.transition(yellow,green,55)
        self.pallette += self.transition(green,blue,55)
        self.pallette += self.transition(blue,green,55)
        self.pallette += self.transition(green,yellow,55)
        self.pallette += self.transition(yellow,orange,55)
        self.pallette += self.transition(orange,red,55)

        self.pallette[-1] = (0,0,0)

    def paintPixels(self, response):
        for pixel in response:
            xi, yi, color = pixel
            self.pixels[xi,yi] = self.pallette[color]

    # Repetition is finished when all divisions have returned.
    def is_repetition_finished(self, ):
        return self.numReplies == self.numDivs

    # Simulation is finished when all images have been generated.
    def is_simulation_finished(self, ):
        return self.repetitions_finished > self.maxRepetitions

    # Definition of how to send divide tasks
    def task_generator(self):
        taskID = 0
        for x in range(self.tiling[0]):
            print("Giving column tile {}".format(x))
            for y in range(self.tiling[1]):
                taskID += 1
                #Send over the tile row, column, number of itterations to quit at,
                # the tiling structure, resolution, and window information.
                yield(taskID,(x,y, self.maxItters,self.tiling,
                      (self.rx,self.ry), self.window.toList()))

    #Reset the response counter for the next itteration
    def reset_responses(self):
        self.numReplies = 0

    #Zoom in and save current image
    def set_next_repetition(self):
        if self.repetitions_finished != 0:
            self.w /= self.nestingFactor
            self.h /= self.nestingFactor
            self.window = Window((self.x, self.y),(self.w, self.h))
            fileName = self.outName.format(self.repetitions_finished)
            self.saveImage(fileName)

    #Put response into the job Buffer, increment number of replies received.
    def record_response(self, task, response):
        with self.responseLock:
            self.numReplies += 1
            self.job_buffer.append((task, response))

    #Process a job by painting pixels
    def record_job(self, job):
        task, response = job
        taskID, taskData = task
        with self.responseLock:
            response = eval(response)
            self.paintPixels(response)

#This class defines a region in the plane that makes up an image.
class Window:
    def __init__(self, pos, size):
        self.x, self.y = pos
        self.w, self.h = size
        self.left = (self.x - self.w/2.0)
        self.right = (self.x + self.w/2.0)
        self.top = (self.y - self.h/2.0)
        self.bottom = (self.y + self.h/2.0)

    #Used to package up this info to go into a task.
    def toList(self):
        return [self.x, self.y, self.w, self.h]

if __name__ == '__main__':
    main()