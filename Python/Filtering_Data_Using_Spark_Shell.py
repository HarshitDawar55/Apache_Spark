#Creating a File
file = open(“Harshit_Hobbies.txt”, “w”)

#Add data in it
file.write(“Games”)

# Create a Spark variable for textfile
textfile = sc.textFile(“Users/HarshitDawar/Downloads/Harshit_Hobbies.txt”)

#Count Elements in the file
textfile.count()

#Print the first element by
textfile.first()

# Print the occurence of each word 
line = textfile.filter(lambda line: “Games” in line)