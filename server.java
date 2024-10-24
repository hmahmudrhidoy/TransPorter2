import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class server {
    private final int port;
    private final int cachePort;
    private final String cacheIp;
    private final String protocol;
    private final ExecutorService executor;
    private final CacheManager cacheManager;

    public server(int port, String protocol, String cacheIp, int cachePort) throws IOException {
        this.port = port;
        this.protocol = protocol.toLowerCase();
        this.cacheIp = cacheIp;
        this.cachePort = cachePort;
        this.cacheManager = new CacheManager("server_files");
        this.executor = Executors.newCachedThreadPool();
    }

    public void start() throws IOException {
        Files.createDirectories(Paths.get("server_files"));
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started on port " + port + " using protocol: " + protocol.toUpperCase());
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    executor.execute(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    System.err.println("Failed to accept client connection.");
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try (Transport transport = createTransport(socket)) {
            String command;
            while ((command = transport.receive()) != null) {
                if ("quit".equalsIgnoreCase(command)) {
                    System.out.println("Client has disconnected.");
                    break;
                } else if (command.startsWith("put ")) {
                    handlePut(command.substring(4).trim(), transport);
                } else if (command.startsWith("get ")) {
                    handleGet(command.substring(4).trim(), transport);
                } else {
                    transport.send("Unknown command");
                    System.err.println("Received unknown command: " + command);
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client request.");
        }
    }

    private void handlePut(String filename, Transport transport) {
        System.out.println("Received PUT request for: " + filename);
        try {
            transport.send("READY");
            String sizeStr = transport.receive();
            long fileSize = Long.parseLong(sizeStr);
            transport.send("SIZE_RECEIVED");
            byte[] data = transport.receiveFile();
            Path filePath = Paths.get("server_files", filename);
            Files.write(filePath, data);
            transport.send("UPLOAD_SUCCESS");
            System.out.println("File '" + filename + "' received and saved.");
        } catch (IOException | NumberFormatException e) {
            System.err.println("Error during file upload.");
        }
    }

    private void handleGet(String filename, Transport transport) {
        System.out.println("Received GET request for: " + filename);
        byte[] data = null;
        String deliverySource = null;

        data = getFileFromCache(filename);

        if (data != null) {
            deliverySource = "cache";
            System.out.println("File delivered from cache.");
        } else {
            Path serverFilePath = Paths.get("server_files", filename);
            if (Files.exists(serverFilePath)) {
                try {
                    data = Files.readAllBytes(serverFilePath);
                    deliverySource = "server";
                    System.out.println("File delivered from server.");
                    storeFileInCache(filename, data);
                } catch (IOException e) {
                    System.err.println("Error reading file from server.");
                }
            } else {
                System.out.println("File not found on server: " + filename);
            }
        }

        try {
            if (data != null) {
                transport.send("READY");
                transport.send(deliverySource);
                transport.sendFile(data);
            } else {
                transport.send("ERROR: File '" + filename + "' not found on server.");
            }
        } catch (IOException e) {
            System.err.println("Error during file delivery.");
        }
    }

    private byte[] getFileFromCache(String filename) {
        System.out.println("Attempting to retrieve file from cache: " + filename);
        try (Socket cacheSocket = new Socket(cacheIp, cachePort);
             DataInputStream dataIn = new DataInputStream(new BufferedInputStream(cacheSocket.getInputStream()));
             DataOutputStream dataOut = new DataOutputStream(new BufferedOutputStream(cacheSocket.getOutputStream()))) {

            dataOut.writeUTF("GET " + filename);
            dataOut.flush();
            String response = dataIn.readUTF();
            if ("FOUND".equals(response)) {
                int size = dataIn.readInt();
                byte[] data = new byte[size];
                dataIn.readFully(data);
                return data;
            } else {
                System.out.println("File not found in cache: " + filename);
                return null;
            }
        } catch (IOException e) {
            System.err.println("Error communicating with cache service.");
            return null;
        }
    }

    private void storeFileInCache(String filename, byte[] data) {
        System.out.println("Storing file in cache: " + filename);
        try (Socket cacheSocket = new Socket(cacheIp, cachePort);
             DataInputStream dataIn = new DataInputStream(new BufferedInputStream(cacheSocket.getInputStream()));
             DataOutputStream dataOut = new DataOutputStream(new BufferedOutputStream(cacheSocket.getOutputStream()))) {

            dataOut.writeUTF("STORE " + filename);
            dataOut.flush();
            dataOut.writeInt(data.length);
            dataOut.flush();
            dataOut.write(data);
            dataOut.flush();
            String response = dataIn.readUTF();
            if ("STORED".equals(response)) {
                System.out.println("File '" + filename + "' stored in cache.");
            } else {
                System.err.println("Failed to store file '" + filename + "' in cache.");
            }
        } catch (IOException e) {
            System.err.println("Error communicating with cache service.");
        }
    }

    private Transport createTransport(Socket socket) throws IOException {
        switch (protocol) {
            case "tcp":
                return new tcp_transport(socket);
            case "snw":
                return new snw_transport(socket);
            default:
                System.err.println("Unknown protocol: " + protocol);
                socket.close();
                throw new IllegalArgumentException("Unknown protocol: " + protocol);
        }
    }

    public static void main(String[] args) {
        try {
            int port = 10000;
            String protocol = "tcp";
            String cacheIp = "localhost";
            int cachePort = 20000;

            if (args.length >= 1) {
                port = Integer.parseInt(args[0]);
            }
            if (args.length >= 2) {
                protocol = args[1];
            }
            if (args.length >= 3) {
                cacheIp = args[2];
            }
            if (args.length >= 4) {
                cachePort = Integer.parseInt(args[3]);
            }

            server serverInstance = new server(port, protocol, cacheIp, cachePort);
            serverInstance.start();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.out.println("Usage: java Server [port] [protocol] [cache ip] [cache port]");
        } catch (IOException e) {
            System.err.println("IO Exception occurred while starting the server.");
            System.out.println("Usage: java Server [port] [protocol] [cache ip] [cache port]");
        }
    }
}