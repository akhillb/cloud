import sys
import os

directory = sys.argv[1]

files = os.listdir(directory)
output = open("out","w")

for f in files:
    if f[-3:] != "crc" and f[-3:] != "swp":
	path = os.path.join(directory,f)
	op = open(path,"r")
	lines = op.readlines()
	start = False
	subject = ""
	for line in lines:
	    if start == False:
		if line[0:9] == "Subject: ":
		    start = True
		    l = line.strip()
		    subject = subject + l[8:]
		else:
		    continue
	    else:
		if line[0:4] == "Mime":
		    start = False
		    output.write(f + " : " + subject + "\n") 
		    break
		else:
		    subject = subject + " " + line.strip()
	op.close()
output.close()
