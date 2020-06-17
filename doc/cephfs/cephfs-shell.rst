
=============
CephFS Shell
=============

CephFS Shell provides shell-like commands that directly interact with the
:term:`Ceph File System`.

This tool can be used in interactive mode as well as in non-interactive mode.
In former mode, cephfs-shell opens a shell session and after the given command
is finished, it prints the prompt string and waits indefinitely. When the
shell session is finished, cephfs-shell quits with the return value of last
executed command. In non-interactive mode, cephfs-shell issues a command and
exits right after the command's execution is complete with the command's return
value.

Behaviour of CephFS Shell can be tweaked using cephfs-shell.conf. CephFS Shell
looks for it by default at the path provided in environment variable
`CEPHFS_SHELL_CONF` and then in user's home directory
(`~/.cephfs-shell.conf`).

Usage :

    cephfs-shell [options] [command]
    cephfs-shell [options] -- [command, command,...]

Options :
    -c, --config FILE     Path to cephfs-shell.conf
    -b, --batch FILE      Path to batch file.
    -t, --test FILE       Path to transcript(s) in FILE for testing


.. note::

    Latest version of the cmd2 module is required for running cephfs-shell.
    If CephFS is installed through source, execute cephfs-shell in the build
    directory. It can also be executed as following using virtualenv:

.. code:: bash

    [build]$ virtualenv -p python3 venv && source venv/bin/activate && pip3 install cmd2
    [build]$ source vstart_environment.sh && source venv/bin/activate && python3 ../src/tools/cephfs/cephfs-shell

Commands
========

mkdir
-----

Create the directory(ies), if they do not already exist.

Usage : 
        
    mkdir [-option] <directory>... 

* directory - name of the directory to be created.

Options :
  -m MODE    Sets the access mode for the new directory.
  -p, --parent         Create parent directories as necessary. When this option is specified, no error is reported if a directory already exists.
 
put
---

Copy a file/directory to Ceph File System from Local File System.

Usage : 
    
        put [options] <source_path> [target_path]

* source_path - local file/directory path to be copied to cephfs.
    * if `.` copies all the file/directories in the local working directory.
    * if `-`  Reads the input from stdin. 

* target_path - remote directory path where the files/directories are to be copied to.
    * if `.` files/directories are copied to the remote working directory.

Options :
   -f, --force        Overwrites the destination if it already exists.


get
---
 
Copy a file from Ceph File System to Local File System.

Usage : 

    get [options] <source_path> [target_path]

* source_path - remote file/directory path which is to be copied to local file system.
    * if `.` copies all the file/directories in the remote working directory.
                    
* target_path - local directory path where the files/directories are to be copied to.
    * if `.` files/directories are copied to the local working directory. 
    * if `-` Writes output to stdout.

Options:
  -f, --force        Overwrites the destination if it already exists.

ls
--

List all the files and directories in the current working directory.

Usage : 
    
    ls [option] [directory]...

* directory - name of directory whose files/directories are to be listed. 
    * By default current working directory's files/directories are listed.

Options:
  -l, --long	    list with long format - show permissions
  -r, --reverse     reverse sort     
  -H                human readable
  -a, -all          ignore entries starting with .
  -S                Sort by file_size


cat
---

Concatenate files and print on the standard output

Usage : 

    cat  <file>....

* file - name of the file

cd
--

Change current working directory.

Usage : 
    
    cd [directory]
        
* directory - path/directory name. If no directory is mentioned it is changed to the root directory.
    * If '.' moves to the parent directory of the current directory.

cwd
---

Get current working directory.
 
Usage : 
    
    cwd


quit/Ctrl + D
-------------

Close the shell.

chmod
-----

Change the permissions of file/directory.
 
Usage : 
    
    chmod <mode> <file/directory>

mv
--

Moves files/Directory from source to destination.

Usage : 
    
    mv <source_path> <destination_path>

rmdir
-----

Delete a directory(ies).

Usage : 
    
    rmdir <directory_name>.....

rm
--

Remove a file(es).

Usage : 
    
    rm <file_name/pattern>...


write
-----

Create and Write a file.

Usage : 
        
        write <file_name>
        <Enter Data>
        Ctrl+D Exit.

lls
---

Lists all files and directories in the specified directory.Current local directory files and directories are listed if no     path is mentioned

Usage: 
    
    lls <path>.....

lcd
---

Moves into the given local directory.

Usage : 
    
    lcd <path>

lpwd
----

Prints the absolute path of the current local directory.

Usage : 
    
    lpwd


umask
-----

Set and get the file mode creation mask 

Usage : 
    
    umask [mode]

alias
-----

Define or display aliases

Usage: 

    alias [name] | [<name> <value>]

* name - name of the alias being looked up, added, or replaced
* value - what the alias will be resolved to (if adding or replacing) this can contain spaces and does not need to be quoted

run_pyscript
------------

Runs a python script file inside the console

Usage: 
    
    run_pyscript <script_path> [script_arguments]

* Console commands can be executed inside this script with cmd ("your command")
  However, you cannot run nested "py" or "pyscript" commands from within this
  script. Paths or arguments that contain spaces must be enclosed in quotes

.. note:: This command is available as ``pyscript`` for cmd2 versions 0.9.13
   or less.

py
--

Invoke python command, shell, or script

Usage : 

        py <command>: Executes a Python command.
        py: Enters interactive Python mode.

shortcuts
---------

Lists shortcuts (aliases) available

Usage :

    shortcuts

history
-------

View, run, edit, and save previously entered commands.

Usage : 
    
    history [-h] [-r | -e | -s | -o FILE | -t TRANSCRIPT] [arg]

Options:
   -h             show this help message and exit
   -r             run selected history items
   -e             edit and then run selected history items
   -s             script format; no separation lines
   -o FILE        output commands to a script file
   -t TRANSCRIPT  output commands and results to a transcript file

unalias
-------

Unsets aliases
 
Usage : 
    
    unalias [-a] name [name ...]

* name - name of the alias being unset

Options:
   -a     remove all alias definitions

set
---

Sets a settable parameter or shows current settings of parameters.

Usage : 

    set [-h] [-a] [-l] [settable [settable ...]]

* Call without arguments for a list of settable parameters with their values.

Options :
  -h     show this help message and exit
  -a     display read-only settings as well
  -l     describe function of parameter

edit
----

Edit a file in a text editor.

Usage:  

    edit [file_path]

* file_path - path to a file to open in editor

run_script
----------

Runs commands in script file that is encoded as either ASCII or UTF-8 text.
Each command in the script should be separated by a newline.

Usage:  
    
    run_script <file_path>


* file_path - a file path pointing to a script

.. note:: This command is available as ``load`` for cmd2 versions 0.9.13
   or less.

shell
-----

Execute a command as if at the OS prompt.

Usage:  
    
    shell <command> [arguments]

locate
------

Find an item in File System

Usage:

     locate [options] <name>

Options :
  -c       Count number of items found
  -i       Ignore case 

stat
------

Display file status.

Usage :

     stat [-h] <file_name> [file_name ...]

Options :
  -h     Shows the help message

snap
----

Create or Delete Snapshot

Usage:

     snap {create|delete} <snap_name> <dir_name>

* snap_name - Snapshot name to be created or deleted

* dir_name - directory under which snapshot should be created or deleted

setxattr
--------

Set extended attribute for a file

Usage :

     setxattr [-h] <path> <name> <value>

*  path - Path to the file

*  name - Extended attribute name to get or set

*  value - Extended attribute value to be set

Options:
  -h, --help   Shows the help message

getxattr
--------

Get extended attribute value for the name associated with the path

Usage :

     getxattr [-h] <path> <name>

*  path - Path to the file

*  name - Extended attribute name to get or set

Options:
  -h, --help   Shows the help message

listxattr
---------

List extended attribute names associated with the path

Usage :

     listxattr [-h] <path>

*  path - Path to the file

Options:
  -h, --help   Shows the help message

df
--

Display amount of available disk space

Usage :

    df [-h] [file [file ...]]

* file - name of the file

Options:
  -h, --help   Shows the help message

du
--

Show disk usage of a directory

Usage :

    du [-h] [-r] [paths [paths ...]]

* paths - name of the directory

Options:
  -h, --help   Shows the help message

  -r     Recursive Disk usage of all directories


quota
-----

Quota management for a Directory

Usage :

    quota [-h] [--max_bytes [MAX_BYTES]] [--max_files [MAX_FILES]] {get,set} path

* {get,set} - quota operation type.

* path - name of the directory.

Options :
  -h, --help   Shows the help message

  --max_bytes MAX_BYTES    Set max cumulative size of the data under this directory

  --max_files MAX_FILES    Set total number of files under this directory tree
