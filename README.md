# This is example code to test streaming.
# Stream.scala is an Actor that reads a file from the local disk and streams out each row at a set interval (Hardcoded in the code)
# job.scala starts the actor, so that the input stream is available for processing.
# Job.scala opens the spark streaming context, starts processing the stream (instantiates a data frame using a custom schema created )
# Note that the whole package need to be 'packaged' using sbt package command and supplied to spark conf, so that spark context have 
# access to all the classes in the package.
# Hope you have fun!! Ram Ram.
