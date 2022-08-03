# BLS

### BLS repo contains the following:
 - main.py: main script to run the program. __See remark No.1__
 - utils.py: contains the functions incluid the "main" function. 
 - requirements.txt: contains the used packages. 
 - args.txt: __See remark No.2__
 - output.parquet
 
 ### Marks:
 - __mark 1__: make sure to update the path for your own local environement in args.txt file.
 - __mark 2__: the args.txt is defined as follow:
 
 >> Path


 >> seriesID1,seriesID2,seriesID3


 >> startyear


 >> endyear


 >> --Raw


 >> --Mirror

* _Raw and Mirror are optional, if any is missing the flag is False, otherwise True_.
* _For the seriesID's, if there is duplicate the code will took the unique series ID's._ __In addition make sure not the give any spaces__.
 
 
