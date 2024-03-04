# Videogram

## Peer-to-Peer Video Chatting 

## Overview
V1deo-gram is a project aimed at implementing a peer-to-peer video chatting model using various tools including Redis, Kafka, and others. The system facilitates real-time video communication between two or more participants in a peer-to-peer fashion.

## Features
- Peer-to-peer video communication.
- Scalable architecture using Redis and Kafka.
- Real-time messaging for signaling.

## Architecture
The system consists of the following components:
1. **Signaling Server**: Handles signaling between peers for establishing WebRTC connections.
2. **Redis**: Used for signaling message exchange between peers.
3. **Kafka**: Provides a distributed messaging system for handling metadata and other communication needs.
4. **WebRTC**: Enables real-time communication between peers in the browser.
5. **Frontend Application**: User interface for initiating and participating in video calls.

## Usage
- Open the frontend application in a web browser.
- Enter your username and initiate a call.
- Share the call link with the participant.
- Once the participant joins, you can start the video call.

## Contributing
We welcome contributions to V1deo-gram! To contribute, please follow these steps:

1. Fork the repository and clone it to your local machine.
2. Create a new branch for your feature or bug fix: `git checkout -b feature/new-feature`.
3. Make your changes and test them thoroughly.
4. Commit your changes: `git commit -m "Add new feature"`.
5. Push to the branch: `git push origin feature/new-feature`.
6. Create a new pull request on GitHub.

Please ensure that your contributions adhere to the project's coding standards and include relevant tests if applicable.



## License
This project is licensed under the [MIT License](LICENSE).
