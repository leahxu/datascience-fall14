cat cmsc.txt | awk -F',' 'BEGIN {printf "Course No., Section No., Instructor, Seats, Open, Waitlist, Days, Time, Bldg., Room No."} 
	/^CMSC/ {print combined; combined = $0} 
    !/^CMSC/ {combined = combined", "$0;}
    END {print combined}' | sed 's/Seats (Total: //g; s/Open: //g; 
    s/Waitlist: //g; s/)//g; s/  /, /g; s/, $//g; 
    s/MWF/MWF,/g; s/MW /MW, /g; s/TuTh/TuTh,/g; s/M /M, /g; 
    s/Tu /Tu, /g; s/W /W, /g; s/Th /Th, /g; s/F /F, /g; s/ ,/,/g' > partII_cmsc.csv

cat worldcup.txt | sed '
	s/|align=center|{{sort dash}}/none/g
	s/||{{sort dash}}//g; 
	s/|-//g; 
	s/|style="background:#fff68f"//g; 
	s/!.*finishes//g; 
	s/|style=white-space:nowrap//g; 
	s/<.*sup>//g; 
	s/\[\[#1|\*\]\]//g; 
	s/GER[[#2|^]]//g; 
	s/|.$//g; 
	s/|[[:digit:]]|//g; 
	s/[[:digit:]]* FIFA World Cup|//g;
	s/\[\[#2\|\^\]\]//g; 
	s/|11|//g; 
	s/|13||12|//g; 
	s/\|{{fb\|//g; 
	s/|[0-9]//g; 
	s/|//g; s/}//g; 
	s/\[//g; 
	s/\]//g; 
	s/(//g; 
	s/)//g; 
	s/, /,/g; 
	s/ //g' | sed '/^$/d' | awk '
	/^[A-Z]/ {team = $1}
	!/^[[A-Z]/ {print team; split($0,a,","); 
	for (i=1; i<=5; i++) {
		if (a[i] != 0) 
			printf("%d, %d\n", a[i], (NR-1)%5)
	}
	}' | awk '/^[A-Z]/ {team = $1} /^[1-9]/ {print team", "$0}' > partII_worldcup1.csv
