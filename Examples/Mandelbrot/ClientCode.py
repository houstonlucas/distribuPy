["task","Window", "mandelBrot", "transform"]
CLIENTDELIM

def task(self, tasks):
    tasks = tasks.split("_")
    task = eval(tasks[0])

    # Extract info from task
    px = task[0]
    py = task[1]
    maxItters = task[2]
    tiling = task[3]
    self.rx,self.ry = task[4]
    windowInfo = task[5]
    window = self.Window(windowInfo[:2], windowInfo[2:])

    #list to contain pixel values
    pixels = []
    nx,ny = tiling
    dx = self.rx/nx
    dy = self.ry/ny
    #Loop over our tiles
    for xi in range(px*dx, ((px+1)*dx)):
        for yi in range(py*dy, ((py+1)*dy)):
            #Calculate color value and store it in pixels
            color = self.mandelBrot(self, (xi,yi), window, maxItters)
            pixels.append((xi,yi,color))

    #Send back the pixels array
    return str(pixels)

#Blatently copied from wikipedia page on mandelbrot set
def mandelBrot(self, pixel, window, maxItters = 100):
    startX = self.transform(pixel[0], 0.0, self.rx, window.left, window.right)
    startY = self.transform(pixel[1], 0.0, self.ry, window.top, window.bottom)
    x, y = 0.0, 0.0

    itteration = 0
    while(x*x + y*y < 2*2 and itteration < maxItters):
        t = x*x - y*y + startX
        y = 2.0*x*y + startY
        x = t
        itteration += 1
    return itteration

#Transforms val linearly mapping old min/max to new min/max
def transform(val, oldMin, oldMax, newMin, newMax):
    oldDiff = float(oldMax-oldMin)
    newDiff = float(newMax-newMin)
    val = newMin + newDiff*((val-oldMin)/oldDiff)
    return val

#Defines region that will make up the image.
class Window:
    def __init__(self, pos, size):
        self.x, self.y = pos
        self.w, self.h = size
        self.left = (self.x - self.w/2.0)
        self.right = (self.x + self.w/2.0)
        self.top = (self.y - self.h/2.0)
        self.bottom = (self.y + self.h/2.0)

    def divide(self, divisions):
        dx,dy = divisions
        nw = self.w/dx
        nh = self.h/dy

        windows = []
        for i in range(dx):
            for j in range(dy):
                windows.append(Window(
                                (self.left+nw*(i+0.5), self.top+nh*(j+0.5)),
                                (nw,nh)))
        return windows
