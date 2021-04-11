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