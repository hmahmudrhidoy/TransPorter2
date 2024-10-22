import java.io.*;
import java.net.*;
import java.nio.file.*;

public class Server {
    private static final int PORT = 12345;
    private Cache cache;

    public Server() {
        cache = new Cache();
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server started on port " + PORT);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                // Use virtual threads (Java 21) or regular threads
                Thread.startVirtualThread(() -> {
                    try {
                        handleClient(clientSocket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    private void handleClient(Socket socket) throws IOException {
        try (Transport transport = new TcpTransport(socket)) {
            // Or use Stop-and-Wait:
            // Transport transport = new SnwTransport(socket);
            String command;
            while ((command = transport.receive()) != null) {
                if (command.startsWith("put ")) {
                    String filename = command.substring(4).trim();
                    transport.send("READY");
                    // Receive file size from client
                    String sizeStr = transport.receive();
                    long fileSize = Long.parseLong(sizeStr);
                    // Send acknowledgment
                    transport.send("SIZE_RECEIVED");
                    byte[] data = transport.receiveFile(fileSize);
                    // Save to server_files
                    Files.write(Paths.get("server_files/" + filename), data);
                    // Cache the file
                    cache.add(filename, data);
                    // Log cache addition
                    System.out.println("Cache updated: Added '" + filename + "' to cache.");
                    transport.send("UPLOAD_SUCCESS");
                    System.out.println("File '" + filename + "' received and saved.");
                } else if (command.startsWith("get ")) {
                    String filename = command.substring(4).trim();
                    byte[] data = null;
                    if (cache.exists(filename)) {
                        data = cache.get(filename);
                        System.out.println("Cache hit: Retrieved '" + filename + "' from cache.");
                    } else if (Files.exists(Paths.get("server_files/" + filename))) {
                        data = Files.readAllBytes(Paths.get("server_files/" + filename));
                        cache.add(filename, data); // Add to cache
                        System.out.println("Cache miss: Loaded '" + filename + "' from disk and added to cache.");
                    }

                    if (data != null) {
                        transport.send("READY");
                        transport.send(String.valueOf(data.length)); // Send file size
                        // Wait for client acknowledgment
                        String clientAck = transport.receive();
                        if (!"SIZE_RECEIVED".equals(clientAck)) {
                            System.out.println("Client did not acknowledge file size.");
                            continue;
                        }
                        transport.sendFile(data);
                        System.out.println("File '" + filename + "' sent to client.");
                    } else {
                        transport.send("ERROR: File '" + filename +
                                "' not found on server.");
                        System.out.println("File '" + filename +
                                "' not found on server.");
                    }
                } else {
                    transport.send("Unknown command");
                    System.out.println("Received unknown command: " + command);
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Server server = new Server();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
