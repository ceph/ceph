module notification-deletion/notification-deletion-example

go 1.18

//replace notification-deletion/notification => ../notification
replace notification => ../notification-deletion
require github.com/aws/aws-sdk-go v1.44.91

require github.com/jmespath/go-jmespath v0.4.0 // indirect
