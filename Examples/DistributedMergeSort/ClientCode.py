["task"]
CLIENTDELIM

def task(self, tasks):
    tasks = tasks.split("_")    
    reply = ""
    for task in tasks:
        data = eval(task)
        data.sort()
        reply +=  str(data) + "_"
    return reply[:-1]