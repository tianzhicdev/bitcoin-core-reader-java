#!/bin/bash

# Navigate to the project directory
cd /Users/biubiu/projects/bitcoin-core-reader-java

# Run git push with a message
git gp "update"

# Use sshpass to connect to the remote host
sshpass -p "$MINI_PW" ssh tianzhichen@marcus-mini.is-very-nice.org << 'ENDSSH'
  # Navigate to the project directory on the remote host
  cd /Users/tianzhichen/projects/bitcoin-core-reader-java

  # Pull the latest changes and build the project
  git pull origin main && mvn clean package

  # Reload the launchctl service
  launchctl unload com.user.java.blocks.plist
  launchctl load com.user.java.blocks.plist
ENDSSH

