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

# Wrap  rows where data starts with '|{{fb|'
w.add(dw.Wrap(column=[],
              table=0,
              status="active",
              drop=False,
              row=dw.Row(column=[],
             table=0,
             status="active",
             drop=False,
             conditions=[dw.StartsWith(column=[],
                    table=0,
                    status="active",
                    drop=False,
                    lcol="data",
                    value="|{{fb|",
                    op_str="starts with")])))

# Extract from wrap between ' any lowercase word |' and '}'
w.add(dw.Extract(column=["wrap"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="}",
                 after="[a-z]+\\|",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Set  extract  name to  Country
w.add(dw.SetName(column=["extract"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Country"],
                 header_row=None))

# Set  Country  name to  Team
w.add(dw.SetName(column=["Country"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["Team"],
                 header_row=None))

# Set  wrap1  name to  1
w.add(dw.SetName(column=["wrap1"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["1"],
                 header_row=None))

# Set  wrap2  name to  2
w.add(dw.SetName(column=["wrap2"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["2"],
                 header_row=None))

# Set  wrap3  name to  3
w.add(dw.SetName(column=["wrap3"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["3"],
                 header_row=None))

# Set  wrap4  name to  4
w.add(dw.SetName(column=["wrap4"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["4"],
                 header_row=None))

# Drop wrap
w.add(dw.Drop(column=["wrap"],
              table=0,
              status="active",
              drop=True))

# Extract from 1 between '[\[' and ']'
w.add(dw.Extract(column=["_1"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after="\\[\\[",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from 1 on '2002'
w.add(dw.Extract(column=["_1"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on="2002",
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from 1 between positions 36, 40
w.add(dw.Extract(column=["_1"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[36,40]))

# Extract from 1 between positions 26, 30
w.add(dw.Extract(column=["_1"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[26,30]))

# Extract from 1 between positions 16, 20
w.add(dw.Extract(column=["_1"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[16,20]))

# Extract from 1 between positions 6, 10
w.add(dw.Extract(column=["_1"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[6,10]))

# Set  extract5  name to  1
w.add(dw.SetName(column=["extract5"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["1"],
                 header_row=None))

# Set  extract4  name to  1
w.add(dw.SetName(column=["extract4"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["1"],
                 header_row=None))

# Set  1  name to  
w.add(dw.SetName(column=["_1"],
                 table=0,
                 status="active",
                 drop=True,
                 names=[""],
                 header_row=None))

# Set  11  name to  first1
w.add(dw.SetName(column=["_11"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["first1"],
                 header_row=None))

# Set  12  name to  first2
w.add(dw.SetName(column=["_12"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["first2"],
                 header_row=None))

# Set  extract3  name to  first3
w.add(dw.SetName(column=["extract3"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["first3"],
                 header_row=None))

# Set  extract2  name to  first4
w.add(dw.SetName(column=["extract2"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["first4"],
                 header_row=None))

# Set  extract1  name to  first5
w.add(dw.SetName(column=["extract1"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["first5"],
                 header_row=None))

# Drop 
w.add(dw.Drop(column=[""],
              table=0,
              status="active",
              drop=True))

# Drop extract
w.add(dw.Drop(column=["extract"],
              table=0,
              status="active",
              drop=True))

# Extract from 2 on '2002'
w.add(dw.Extract(column=["_2"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on="2002",
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from 2 between positions 26, 30
w.add(dw.Extract(column=["_2"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[26,30]))

# Extract from 2 between positions 16, 20
w.add(dw.Extract(column=["_2"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[16,20]))

# Extract from 2 between positions 6, 10
w.add(dw.Extract(column=["_2"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[6,10]))

# Drop 2
w.add(dw.Drop(column=["_2"],
              table=0,
              status="active",
              drop=True))

# Set  extract8  name to  second1
w.add(dw.SetName(column=["extract8"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["second1"],
                 header_row=None))

# Set  extract7  name to  second2
w.add(dw.SetName(column=["extract7"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["second2"],
                 header_row=None))

# Set  extract6  name to  second3
w.add(dw.SetName(column=["extract6"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["second3"],
                 header_row=None))

# Set  extract  name to  second4
w.add(dw.SetName(column=["extract"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["second4"],
                 header_row=None))

# Extract from 3 on '2010'
w.add(dw.Extract(column=["_3"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on="2010",
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from 3 between positions 26, 30
w.add(dw.Extract(column=["_3"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[26,30]))

# Extract from 3 between ' [\[' and ']]'
w.add(dw.Extract(column=["_3"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]]",
                 after=" \\[\\[",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from 3 between '[\[' and ']'
w.add(dw.Extract(column=["_3"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after="\\[\\[",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Drop 3
w.add(dw.Drop(column=["_3"],
              table=0,
              status="active",
              drop=True))

# Set  extract11  name to  third1
w.add(dw.SetName(column=["extract11"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["third1"],
                 header_row=None))

# Set  extract10  name to  third2
w.add(dw.SetName(column=["extract10"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["third2"],
                 header_row=None))

# Set  extract9  name to  third3
w.add(dw.SetName(column=["extract9"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["third3"],
                 header_row=None))

# Set  extract  name to  third4
w.add(dw.SetName(column=["extract"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["third4"],
                 header_row=None))

# Extract from 4 between positions 26, 30
w.add(dw.Extract(column=["_4"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=None,
                 before=None,
                 after=None,
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=[26,30]))

# Extract from 4 between ' [\[' and ']'
w.add(dw.Extract(column=["_4"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after=" \\[\\[",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Extract from 4 between '[\[' and ']'
w.add(dw.Extract(column=["_4"],
                 table=0,
                 status="active",
                 drop=False,
                 result="column",
                 update=False,
                 insert_position="right",
                 row=None,
                 on=".*",
                 before="]",
                 after="\\[\\[",
                 ignore_between=None,
                 which=1,
                 max=1,
                 positions=None))

# Drop 4
w.add(dw.Drop(column=["_4"],
              table=0,
              status="active",
              drop=True))

# Set  extract13  name to  fourth1
w.add(dw.SetName(column=["extract13"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["fourth1"],
                 header_row=None))

# Set  extract12  name to  fourth2
w.add(dw.SetName(column=["extract12"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["fourth2"],
                 header_row=None))

# Set  extract  name to  fourth3
w.add(dw.SetName(column=["extract"],
                 table=0,
                 status="active",
                 drop=True,
                 names=["fourth3"],
                 header_row=None))

w.apply_to_file(sys.argv[1]).print_csv(sys.argv[2])

