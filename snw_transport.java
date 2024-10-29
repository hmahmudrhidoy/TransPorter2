import java.io.*;
import java.net.*;

public class snw_transport implements Transport {
    private final DatagramSocket udpSocket;
    private final InetAddress serverAddress;
    private final int serverPort;
    private static final int CHUNK_SIZE = 1000;
    private static final int ACK_TIMEOUT = 1000; // Timeout in milliseconds

    public snw_transport(InetAddress serverAddress, int serverPort, DatagramSocket udpSocket) throws IOException {
        this.udpSocket = udpSocket;
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        udpSocket.setSoTimeout(ACK_TIMEOUT);
    }

    @Override
    public void send(String message) throws IOException {
        byte[] messageBytes = message.getBytes();
        DatagramPacket packet = new DatagramPacket(messageBytes, messageBytes.length, serverAddress, serverPort);
        udpSocket.send(packet);
        waitForAck();
    }

    @Override
    public String receive() throws IOException {
        byte[] buffer = new byte[CHUNK_SIZE];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        udpSocket.receive(packet);
        sendAck();
        return new String(packet.getData(), 0, packet.getLength());
    }

    @Override
    public void sendFile(byte[] data) throws IOException {
        String lengthMessage = "LEN:" + data.length;
        send(lengthMessage);

        int totalSent = 0;
        while (totalSent < data.length) {
            int bytesToSend = Math.min(CHUNK_SIZE, data.length - totalSent);
            DatagramPacket chunkPacket = new DatagramPacket(data, totalSent, bytesToSend, serverAddress, serverPort);
            udpSocket.send(chunkPacket);
            totalSent += bytesToSend;
            try {
                waitForAck();
            } catch (SocketTimeoutException e) {
                System.err.println("Did not receive ACK. Terminating.");
                return;
            }
        }
        send("FIN");
    }

    @Override
    public byte[] receiveFile() throws IOException {
        byte[] lenBuffer = new byte[CHUNK_SIZE];
        DatagramPacket lenPacket = new DatagramPacket(lenBuffer, lenBuffer.length);
        udpSocket.receive(lenPacket);
        String lengthMessage = new String(lenPacket.getData(), 0, lenPacket.getLength());

        if (!lengthMessage.startsWith("LEN:")) {
            throw new IOException("Invalid length message");
        }
        int fileSize = Integer.parseInt(lengthMessage.substring(4));
        sendAck();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int totalReceived = 0;
        while (totalReceived < fileSize) {
            byte[] buffer = new byte[CHUNK_SIZE];
            DatagramPacket chunkPacket = new DatagramPacket(buffer, buffer.length);

            try {
                udpSocket.receive(chunkPacket);
            } catch (SocketTimeoutException e) {
                System.err.println("Data transmission terminated prematurely.");
                return null;
            }

            baos.write(buffer, 0, chunkPacket.getLength());
            totalReceived += chunkPacket.getLength();
            sendAck();
        }

        byte[] finBuffer = new byte[CHUNK_SIZE];
        DatagramPacket finPacket = new DatagramPacket(finBuffer, finBuffer.length);
        udpSocket.receive(finPacket);
        String finMessage = new String(finPacket.getData(), 0, finPacket.getLength());

        if (!"FIN".equals(finMessage)) {
            System.err.println("Did not receive FIN message.");
            return null;
        }
        sendAck();

        return baos.toByteArray();
    }

    @Override
    public void close() {
        udpSocket.close();
    }

    private void waitForAck() throws IOException {
        byte[] ackBuffer = new byte[CHUNK_SIZE];
        DatagramPacket ackPacket = new DatagramPacket(ackBuffer, ackBuffer.length);
        udpSocket.receive(ackPacket);
        String ack = new String(ackPacket.getData(), 0, ackPacket.getLength());
        if (!"ACK".equals(ack)) {
            throw new IOException("ACK not received");
        }
    }

    private void sendAck() throws IOException {
        byte[] ackBytes = "ACK".getBytes();
        DatagramPacket ackPacket = new DatagramPacket(ackBytes, ackBytes.length, serverAddress, serverPort);
        udpSocket.send(ackPacket);
    }
}