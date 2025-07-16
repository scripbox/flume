ExUnit.start()

# Ensure the application is started which will start the Flume supervisor
Mix.Task.run("app.start")

# Load support modules for tests
Code.require_file("test/support/worker.ex")
Code.require_file("test/support/echo_worker.ex")

# The Flume supervisor is already started by the application
# when Mix.Task.run("app.start") is called by tests
