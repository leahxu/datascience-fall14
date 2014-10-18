import re

with open("cmsc.txt", "r") as sources:
    lines = sources.readlines()
with open("partIII_cmsc.csv", "w") as sources:
    sources.write("Course No., Section No., Instructor, Seats, Open, Waitlist, Days, Time, Bldg., Room No.")
    course = ''
    info = ''
    for line in lines:
        if line.find("CMSC") == -1:
            line = re.sub(r'Seats \(Total: ', '', line)
            line = re.sub(r'Open: ', '', line)
            line = re.sub(r'Waitlist: ', '', line)
            line = re.sub(r'\)', '', line)
            line = re.sub(r'MWF', 'MWF,', line)
            line = re.sub(r'MW ', 'MW,', line)
            line = re.sub(r'TuTh', 'TuTh,', line)
            line = re.sub(r'M ', 'M, ', line)
            line = re.sub(r'Tu ', 'Tu, ', line)
            line = re.sub(r'W ', 'W, ', line)
            line = re.sub(r'Th ', 'Th, ', line)
            line = re.sub(r'F ', 'F, ', line)
            line = re.sub(r'  ', ', ', line)
            line = line.rstrip()
            info = info + ', ' + line
        else: 
            sources.write(course.rstrip() + info[:-2] + '\n')
            course = line
            info = ''