from wrangler import dw
import sys

if(len(sys.argv) < 3):
    sys.exit('Error: Please include an input and output file.  Example python script.py input.csv output.csv')

w = dw.DataWrangler()

# Split data repeatedly on newline  into  rows
w.add(dw.Split(column=["data"],
               table=0,
               status="active",
               drop=True,
               result="row",
               update=False,
               insert_position="right",
               row=None,
               on="\n",
               before=None,
               after=None,
               ignore_between=None,
               which=1,
               max=0,
               positions=None,
               quote_character=None))

# Wrap  rows where data contains 'CMSC'
w.add(dw.Wrap(column=[],
              table=0,
              status="active",
              drop=False,
              row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.Contains(column=[],
                  table=0,
                  status="active",
                  drop=False,
                  lcol="data",
                  value="CMSC",
                  op_str="contains")])))

# Drop wrap6, wrap7, wrap8, wrap9...
w.add(dw.Drop(column=["wrap6","wrap7","wrap8","wrap9","wrap10","wrap11","wrap12","wrap13","wrap14","wrap15","wrap16","wrap17","wrap18","wrap19","wrap20","wrap21","wrap22","wrap23","wrap24","wrap25","wrap26","wrap27","wrap28","wrap29","wrap30","wrap31","wrap32","wrap33","wrap34","wrap35","wrap36","wrap37","wrap38","wrap39"],
              table=0,
              status="active",
              drop=True))

# Drop wrap73, wrap40, wrap41, wrap42...
w.add(dw.Drop(column=["wrap73","wrap40","wrap41","wrap42","wrap43","wrap44","wrap45","wrap46","wrap47","wrap48","wrap49","wrap50","wrap51","wrap52","wrap53","wrap54","wrap55","wrap56","wrap57","wrap58","wrap59","wrap60","wrap61","wrap62","wrap63","wrap64","wrap65","wrap66","wrap67","wrap68","wrap69","wrap70","wrap71","wrap72"],
              table=0,
              status="active",
              drop=True))

# Drop wrap74, wrap75, wrap76, wrap77...
w.add(dw.Drop(column=["wrap74","wrap75","wrap76","wrap77","wrap78","wrap79","wrap80","wrap81"],
              table=0,
              status="active",
              drop=True))

# Set  wrap  name to  Course_No.
w.add(dw.SetName(column=["wrap"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Course_No."],
                 header_row=None))

# Set  wrap1  name to  Section_No.
w.add(dw.SetName(column=["wrap1"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Section_No."],
                 header_row=None))

# Set  wrap2  name to  Instructor
w.add(dw.SetName(column=["wrap2"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Instructor"],
                 header_row=None))

# Extract from wrap5 on ' any number '
w.add(dw.Extract(column=["wrap5"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on="\\d+",
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from wrap5 on ' any word '
w.add(dw.Extract(column=["wrap5"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on="[a-zA-Z]+",
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Set  extract1  name to  Buldg.
w.add(dw.SetName(column=["extract1"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Buldg."],
                 header_row=None))

# Set  Buldg.  name to  Bldg.
w.add(dw.SetName(column=["Buldg."],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Bldg."],
                 header_row=None))

# Drop wrap5
w.add(dw.Drop(column=["wrap5"],
              table=0,
              status="active",
              drop=True))

# Extract from wrap4 after ' any word  '
w.add(dw.Extract(column=["wrap4"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before=None,
                 after="[a-zA-Z]+ ",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Set  extract2  name to  Time
w.add(dw.SetName(column=["extract2"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Time"],
                 header_row=None))

# Extract from wrap4 on ' any word '
w.add(dw.Extract(column=["wrap4"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on="[a-zA-Z]+",
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Set  extract3  name to  Days
w.add(dw.SetName(column=["extract3"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Days"],
                 header_row=None))

# Drop wrap4
w.add(dw.Drop(column=["wrap4"],
              table=0,
              status="active",
              drop=True))

# Extract from wrap3 between 'Waitlist: ' and ')'
w.add(dw.Extract(column=["wrap3"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="\\)",
                 after="Waitlist: ",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Set  extract4  name to  Waitlist
w.add(dw.SetName(column=["extract4"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Waitlist"],
                 header_row=None))

# Extract from wrap3 between ' Open: ' and ','
w.add(dw.Extract(column=["wrap3"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before=",",
                 after=" Open: ",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Set  extract5  name to  Open
w.add(dw.SetName(column=["extract5"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Open"],
                 header_row=None))

# Extract from wrap3 between ': ' and ','
w.add(dw.Extract(column=["wrap3"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before=",",
                 after=": ",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Set  extract6  name to  Seats
w.add(dw.SetName(column=["extract6"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Seats"],
                 header_row=None))

# Drop wrap3
w.add(dw.Drop(column=["wrap3"],
              table=0,
              status="active",
              drop=True))

# Set  extract  name to  Room_No.
w.add(dw.SetName(column=["extract"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Room_No."],
                 header_row=None))

w.apply_to_file(sys.argv[1]).print_csv(sys.argv[2])

