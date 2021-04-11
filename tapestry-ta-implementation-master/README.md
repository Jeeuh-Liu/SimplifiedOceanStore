# Instructions for Usage in Puddlestore

1. Clone this repository to a filepath on your local system. 

2. In your Puddlestore `go.mod`, you will see a `replace` keyword. Change the path after `=>` to point to the path you chose in (1). 

That's it! Now all your import statements to `tapestry/pkg` in Puddlestore can work without issue. 