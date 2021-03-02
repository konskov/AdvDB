from io import StringIO
import csv

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def foo():
    return (1,(2,3))
f = open('../movies.csv', 'r')   
lines = f.readlines()
for i,line in enumerate(lines):
    if i < 4:
        print(line)
f.close()        

print(split_complex(lines[0]))
#print(lines[0][0:5])
print(foo())