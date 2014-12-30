import os
import sys

print "Loading..."
inbox = open("./inbox","r")
sent = open("./open","r")

a = {}
b = {}

lines = inbox.readlines()
for line in lines:
    temp = line.strip().split(" ")
    a[temp[0]] = temp[1:]

lines = inbox.readlines()
for line in lines:
    temp = line.strip().split(" ")
    b[temp[0]] = temp[1:]

print "Enter search"
while True:

inbox.close()
sent.close()
