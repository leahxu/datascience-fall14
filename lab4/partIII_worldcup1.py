import re

with open("worldcup.txt", "r") as sources:
    lines = sources.readlines()
with open("partIII_worldcup1.csv", "w") as sources:
    sources.write("Team,Year,Place\n")
    place = 0
    country = ''
    for line in lines:
        line = re.sub(r'align=center\|{{sort dash}}', 'none', line)
        line = re.sub(r'\|\|{{sort dash}}', '', line)
        line = re.sub(r'\|-', '', line)
        line = re.sub(r'!.*finishes', '', line)
        line = re.sub(r'\|style="background:#fff68f"', '', line)
        line = re.sub(r'\|style=white-space:nowrap', '', line)
        line = re.sub(r'<.*sup>', '', line)
        line = re.sub(r'\[\[#1\|\*\]\]', '', line)
        line = re.sub(r'GER\[\[#2\|\^\]\]', '', line)
        line = re.sub(r'\|.$', '', line)
        line = re.sub(r'\d{4} FIFA World Cup\|', '', line)
        line = re.sub(r'\|{{fb\|', '', line)
        line = re.sub(r'}', '', line)
        line = re.sub(r'\|\d(\|\|)*(\d\|)*', '', line)
        line = re.sub(r'\|', '', line)
        line = re.sub(r'\[', '', line)
        line = re.sub(r'\]', '', line)
        line = re.sub(r'\(', '', line)
        line = re.sub(r'\)', '', line)
        line = re.sub(r' ', '', line)
        line = re.sub(r'#2\^', '', line)
        line = line.rstrip()
        
        m = re.match(r'[A-Z]{3}', line)
        if m: 
            country = line

        years = line.split(',')
        if ((len(line) > 1) & (line != country)):
            for year in years: 
                if (year != "none"):
                    sources.write(country + ', ' + year + ', ' + str((place % 4)+1) + '\n')
            place = place + 1