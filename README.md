test machine : CSELAB_machine_name
date : 11 / 02 / 20
name : Mustaf Ahmed , Ellie Hollenberger, John Schmitz
x500 : ahmed719 , holle299, schm4722

Purpose:
The purpose of this program is to take a text file and have a count for each unique word in the file.

How to Compile:
Navigate to the Template directory of the program and enter make. Then run
./mapreduce #mappers #reducers inputfile
Where #mappers is the number of mapper processes, #reducer is the number of reducer processes, and inputfile is your input text file. Expected output will be in the output directory under MapOut and ReduceOut respectively.

What the program does:
The program starts by taking in a text file and the number of mapper and reducer processes as input. It then calls mapper which creates individual text files for every unique word, and in the text file writes a series of 1's that each represent an instance in the text file. Once completed, reducer is called, which takes the individual text files and compiles them into one file per process that contains each unique word and the number of times that word is found in that chunk of the text file.

Assumptions: None

Team:17
John Schmitz schm4722@umn.edu
Mustaf Ahmed ahmed719@umn.edu
Ellie Hollenberger holle299@umn.edu

Contributions:
Our team chose to meet on Zoom for the project, and work through the code togther using Repl.