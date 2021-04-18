# TA Implementation of Puddlestore

### Prerequisites for using Tapestry

You will need to use your own Tapestry implementation or the TA implementation of Tapestry for this project (see the handout for more details). However, you will not be using import statements in Go for this. Instead, you will be using a powerful feature known as [the `replace directive` in Go modules](https://thewebivore.com/using-replace-in-go-mod-to-point-to-your-local-module/)

Steps:

1. Download the Tapestry implementation to a local folder. 
2. Update this line in `go.mod`

```
replace tapestry => /path/to/your/tapestry/implementation/root/folder
```

so that imports of `tapestry` now point to your local folder where you've downloaded Tapestry. 

That's it! When running tests in Gradescope, we will automatically rewrite this line to point to our TA implementation, so you will be tested against our implementation and not be penalized for any issues of your own. 

### Prerequisites for using Zookeeper

With the popular container engine called Docker, installing and running Zookeeper on your local machine is as simple as running

```
docker run --rm -p 2181:2181 zookeeper
```

On Windows, you will need to use Docker for Desktop.

You can also follow manual instructions for installing Zookeeper directly on your platform. 

### Some expectations for using this README

Write your student tests, document them here!

Distribution: Zhengyan Lyu: PuddleStore Client, Tests

Zongjie Liu: PuddleStore Client, Tests

Extra Feature: ACL

Bug: Even Distribution kill failed

Tests:

TestReadEmptyFile: Test reading empty files

TestReadWrite: Test read and write

TestWriteAcrossBlock: Test write across block

TestReadAcrossBlock: Test read across block

TestReadBeyondFileSize: Test Read Beyond File Size

TestClient: Test client

TestTwoClient: Test two clients

TestCreateUnderRoot: Test Create Under Root

TestReadNonexistFile: Test Read Nonexist File

TestNormalClose1: Test close

TestNormalClose2: Test close2

TestReadClosedFile: Test Read Closed File

TestRemove: Test Remove

TestWatch: Test watch

TestCreateRoot: Test create root

TestOpenNonExistWithoutCreate: Test Open NonExist path Without Create

TestWriteEmptyWithOffset: Test Write Empty With Offset

TestReadWrite2: Test read and write

TestList: Test List with mkdir and remove

TestList2: Test List with mkdir

TestRemove2: Test remove

TestMkdir2: Test make dir

TestMkRoot: test make root dir

TestReadBeyondEmpty: test read beyond empty

TestWriteEdges: test write edges

TestMkdir3: Test make dir

TestReadOthersFile: Test read other file

TestOpenSameFile: test two client read same file

TestConWrite: test write concurrently
