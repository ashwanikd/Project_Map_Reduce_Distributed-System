@echo off
setlocal enableextensions
ver
echo ***************STARTING PROGRAM***************

Rem directory of input files
set inputDir=../../../input/

set map=Mapper
set reduce=Reducer
set main=Main

set /a reducerCount=2
set /a port=2001
set /a count=0

Rem running the main server
start java %main%

Rem running each reducer on server
:loop1
if %count%==%reducerCount% goto end1
    set /a count+=1
    start java %reduce%
goto :loop1
:end1

Rem running mappers
Rem each mapper will be running on a different server
for %%f in (%inputDir%*.txt) do (
    start java %map% %inputDir%%%f %reducerCount%
)

Rem This is end of program
echo ***************PROGRAM COMPLETED***************