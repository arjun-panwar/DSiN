from tkinter import *
from tkinter import filedialog
import tkinter.messagebox
import docx
import os
import logging as log

#log configuration
log.basicConfig(filename='File Explorer.log',level = log.DEBUG,format='%(asctime)s -%(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


# tkinter app root configuration
root = Tk()     # Creating tkinter root
root.title('File Explorer')      #title
root.geometry("1500x500")    #root size
root.minsize(500,500)       # minimum size
root.config(background = "white")   #background color

#tinker input and search button
searchkey = StringVar()
e1= Entry(root,textvariable =searchkey,font="Helvetica 16", bd =5)
e1.pack(side=TOP, fill="x")
#button will call function main with search keyword as argument if its not empty
e2 = tkinter.Button(root,text ="Search",font="Helvetica 16 bold", bg='light green',activebackground='#78d6ff',command =lambda :main(str(searchkey.get())) if searchkey.get()!="" else None )
e2.pack(side=TOP, fill="x")

log.info("App launched!")

#global variable label which contains all the tkinter labels that are displayed on the screen
global labels
labels = []
def main(searchkeyword):
    """
    THis main function holds all the major functionality of searching and merging after user press the search button
    :param searchkeyword: search keyword entered by  user
    :return: 
    """
    log.info("Search keyword: " + searchkeyword) #logging search keyword

    # global variable result which store all the search results(file name and path)
    global result
    result = []

    #calling search function by passing searchkeyword and system root path as argument
    log.info("Search Started")  # logging
    search(searchkeyword, "/home/arjun/")
    log.info("Searching Complete")  # logging

    #checking file found or not
    if len(result) == 0:
        alert("File Not Found!!", "red")
        log.warning("File not found!!")  # logging File not found!!
        return
    else:
       log.info("No. of results found: " + str(len(result)))  # logging No. of results found

    #displaying Merge Menu on tkinter app screen
    #tkinter menubar configuration
    mainmenu = Menu(root,background='light green', foreground='black', activebackground='#78d6ff', activeforeground='white')
    m1 = Menu(mainmenu, tearoff=0,background='light green', foreground='black',activebackground='#78d6ff', activeforeground='white')

    #buttons in MERGE menubar TXT and DOCX which will call their respective function with search keyword and result list as argument
    m1.add_command(label="txt",font="Helvetica 24", command=lambda: merge_txt(searchkeyword,result))
    m1.add_command(label="docx",font="Helvetica 24", command=lambda: merge_docx(searchkeyword,result))
    root.config(menu=mainmenu)
    mainmenu.add_cascade(label="MERGE",font="Helvetica 24",  menu=m1)
    log.info("Merge option is now available on app window")  # logging

    display_result() #displaying search result


def display_result():
    """
    this function will display result on app window with alternate colour in each row
    :return:
    """

    clean()  # to clean all the labels from screen if present

    row_no = 1  # variable to store file no. to display on screen
    # loop to iterate through each file in list result
    for i in result:
        out = str(row_no) + ": " + i[0] + "      (" + i[1]  # appending result no. + file name + file path to a string
        #below if case will help us to gain different colours in different line of resulting labels
        colour = "light yellow"
        if row_no % 2 == 0:
            colour = "light pink"

        #displaying tkinter lable
        l = Label(text=out, font="Helvetica 16", bg=colour, fg="black", pady=2, anchor="w")
        l.pack(side=TOP, fill="x")
        labels.append(l)   #appending all the labels to labels list
        row_no += 1 #increasing row count
    log.info("Search Result displayed successfully ")  # logging

def search(key,path):
    """
    this  Recursive function will itterate through each file and folder of my system to search file and store the result in result list
    :param key: Search Keyword
    :param path: Path of directory to be searched
    :return:
    """

    os.chdir(path)  #changing directory to path
    List = os.listdir()   #List store all the files & folder present in that directory
    length=len(List)  #no of files and folder in that directory

    if len(List) == 0: #if directory is empty return from this search function
        return

    #loop to itterate through each file of that directory
    for file in List:

        if file[0] == '.': #if it is a hidden file or folder(starts with .) move to next file
            continue

        if os.path.isdir(path+file+"/")  and len(os.listdir(os.chdir(path + file + "/"))) != 0:
            #if it is a directory and contains 1 or more file
            #recursive call to the search function with search keyword and new path as argument in it
            search(key, path + file + "/")
        else:
            c = check(file, key) #check if file name anyhow matches with search keyword
            if c == 0:
                #if full file name matches, insert search result in beginning of result list
                result.insert(0, [file, os.getcwd()])
            elif c == 1:
                # else if partially file name matches,append it to last of result list
                result.append([file, os.getcwd()])
    #return result list
    return result

def check(filename, searchkeyword):
    """
    this function is used to check searchkeyword in file name
    return 0 if full file name match with search keyword
    return 1 if substring of file name match
    return -1 if file name do not match at all
    """
    try:
        if(filename[0:filename.index(".")].strip()==searchkeyword):
            return 0
    except:
        pass
    if (filename.find(searchkeyword) != -1):
        return 1
    else:
        return -1

def alert(msg,b_colour="red",f_colour="black"):
    """
    This function will clear clear the screen and display Alert/message on app window
    :param msg: message to be displayed
    :param b_colour: background colour of message by default it is red
    :param f_colour: message text(foreground) colour of message by default it is black
    :return:
    """
    clean() #cleaning background
    #tkinter label to display message
    l = Label(text=msg, font="Helvetica 44", bg=b_colour, fg=f_colour, pady=22, anchor="center")
    l.pack(side=TOP, fill=BOTH)
    labels.append(l)  #appending label to labels list

def clean():
    """
    This function is to clean all the labels from the app window
    :return:
    """
    for label in labels:
        label.destroy() #distroying 1 by 1 all the labels in list labels
    labels.clear()  #clearing list labels for further use
    log.info("screen cleared")  #Every time screen is cleared, it is logged into log file



def merge_docx(key,result):
    """
    This function is used to merge all the file in search result having docx extension
    :param key: search keyword,it will be the name of mergerd file
    :param result: search result list
    :return:
    """
    log.info("Merging docx file in search result")
    c = 0 #variable to check if file is created or not
    #creating a file on desktop with name as search keyword name and .docx extension
    f = open("/home/arjun/Desktop/" + key + ".docx", "a+")
    for file in result:
        #iterating through each file in result list
        try:
            if file[0][file[0].index(".") + 1:].lower() == "docx":
                #if file name have .docx extenson append its data to .docx file on desktop
                c = 1
                f.write("\n\n\n\n\n"+file[0]+"\n\n")
                doc = docx.Document(file[1]+"/"+file[0])
                #iterating through each paragraph of filr and appending its data
                for para in doc.paragraphs:
                    f.write(para.text+"\n")
        except:
            pass
    f.close()
    if c == 1:
        alert("File " + key + ".docx " + "has been created on Desktop","green","white")
        log.info("File " + key + ".docx " + "has been created on Desktop")
    else:
        alert(".txt file not found in search result","red")
        log.warning(".txt file not found in search result",)

def merge_txt(key,result):
    """
    This function is used to merge all the file in search result having txt extension
    :param key: search keyword,it will be the name of mergerd file
    :param result: search result list
    :return:
    """
    log.info("Merging txt file in search result")
    c = 0  # variable to check if file is created or not
    # creating a file on desktop with name as search keyword name and .txt extension
    f = open("/home/arjun/Desktop/" + key + ".txt", "a+")
    for file in result:
        #iterating through each file in result list
        try:
            if file[0][file[0].index(".") + 1:].lower()=="txt":
                # if file name have .txt extenson append its data to .txt file on desktop
                c=1
                f.write("\n\n\n\n\n"+file[0]+"\n\n")
                file1 = open(file[1]+"/"+file[0], 'r')
                Lines = file1.readlines()
                # iterating through each paragraph of filr and appending its data
                for line in Lines:
                    f.write(line + "\n")

        except:
            pass
    f.close()
    if c==1:
        alert("File "+key+".txt "+ "has been created on Desktop","green","white")
        log.info("File " + key + ".txt " + "has been created on Desktop")
    else:
        alert(".txt file not found in search result","red")
        log.warning(".txt file not found in search result", )


root.mainloop()
