package org.kalaider.p2p;

import lombok.*;

import java.io.*;
import java.net.*;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    private static final int DATAGRAM_PACKET_SIZE = 1024;
    private static final Duration UDP_CLIENT_TIMEOUT = Duration.ofMinutes(1);

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    private static final class Config {
        private boolean tcp;
        private boolean server;
        private boolean broadcast;
        private int port;
        private String host;
    }

    public static void main(String[] args) throws IOException {
        new Main(args).run();
    }

    private Main(String[] args) {
        Pattern p = Pattern.compile("--(?<name>[\\w.]+)(=(?<value>.+))?");
        Map<String, String> params = new HashMap<>();
        for (String arg : args) {
            Matcher m = p.matcher(arg);
            require(m.matches(), "argument must be in form ''--name=value'', but got: {0}", arg);
            params.put(m.group("name"), m.group("value"));
        }
        this.config = toConfig(params);
    }

    private final Config config;

    private void run() throws IOException {
        if (config.isServer()) runServer();
        else runClient();
    }

    private void runServer() throws IOException {
        require(config.getPort() != 0, "port is required for server profile");
        if (config.isTcp()) {
            new TcpServer(config.getPort()).run();
        } else {
            if (config.isBroadcast()) {
                require(config.getHost() != null, "host is required for broadcasting udp server profile");
                new UdpServer(config.getHost(), config.getPort()).run();
            } else {
                new UdpServer(config.getPort()).run();
            }
        }
    }

    private void runClient() throws IOException {
        require(config.getPort() != 0, "port is required for client profile");
        require(config.getHost() != null, "host is required for client profile");

        if (config.isTcp()) {
            new TcpClient(config.getHost(), config.getPort()).run();
        } else {
            new UdpClient(config.getHost(), config.getPort(), config.isBroadcast()).run();
        }
    }

    private static void require(boolean test, String message, Object... args) {
        if (!test) {
            String msg = MessageFormat.format(message, args);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void forever(InputStream in, Consumer<String> fn) {
        forever(new Scanner(in), fn);
    }

    private static void forever(Scanner r, Consumer<String> fn) {
        while (r.hasNextLine()) {
            fn.accept(r.nextLine());
        }
    }

    private static void log(String msg, Object... args) { System.out.println(MessageFormat.format(msg, args)); }

    private Config toConfig(Map<String, String> args) {
        Config cfg = new Config();
        cfg.setTcp(args.containsKey("tcp") || !args.containsKey("udp"));
        cfg.setServer(args.containsKey("server") || !args.containsKey("client"));
        cfg.setBroadcast(args.containsKey("broadcast"));
        cfg.setPort(Optional.ofNullable(args.get("port")).map(Integer::parseInt).orElse(0));
        cfg.setHost(args.get("host"));
        return cfg;
    }

    private static class TcpServer {

        @Value
        private static class Client {
            private final int id;
            private final Socket socket;
            @Override public String toString() { return id + " @ " + socket.getRemoteSocketAddress(); }
        }

        private final ExecutorService threadPool = Executors.newCachedThreadPool();
        private final Collection<Client> clients = new ConcurrentLinkedDeque<>();
        private final ServerSocket socket;
        private int clientSeq;

        TcpServer(int port) throws IOException {
            this.clientSeq = 0;
            this.socket = new ServerSocket(port);
        }

        void run() {
            threadPool.submit(this::broadcastSystemIn);
            while (true) {
                try {
                    accept();
                } catch (IOException e) {
                    log("exception while waiting for incoming connections: ", e.getMessage());
                    threadPool.shutdownNow();
                    try {
                        threadPool.awaitTermination(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ex) {
                        // ignore
                    }
                    return;
                }
            }
        }

        private void broadcastSystemIn() {
            forever(System.in, line -> {
                clients.forEach(c -> {
                    try {
                        PrintWriter w = new PrintWriter(c.getSocket().getOutputStream(), true);
                        w.println(line);
                        if (w.checkError()) {
                            log("unable to write to socket [{0}]: ", c.toString());
                        }
                    } catch (IOException e) {
                        log("exception while sending message to client [{0}]: ", c.toString(), e.getMessage());
                    }
                });
            });
        }

        private void accept() throws IOException {
            Socket conn = socket.accept();
            ++clientSeq;
            threadPool.submit(() -> handleConnection(conn));
        }

        private void handleConnection(Socket cs) {
            Client c = new Client(clientSeq, cs);
            clients.add(c);
            try {
                handleClient(c);
            } finally {
                clients.remove(c);
            }
        }

        private void handleClient(Client c) {
            try (Socket s = c.getSocket()) {
                log("[{0}] connected", c);
                forever(s.getInputStream(), line -> {
                    log("[{0}] says: {1}", c, line);
                });
                log("[{0}] disconnected normally", c);
            } catch (IOException e) {
                log("[{0}] disconnected abnormally: {1}", c, e.getMessage());
            }
        }
    }

    private static class TcpClient {

        private final ExecutorService threadPool = Executors.newCachedThreadPool();
        private final Socket socket;
        private final PrintWriter socketWriter;
        private final Scanner socketReader;

        TcpClient(String host, int port) throws IOException {
            this.socket = new Socket(host, port);
            log("[{0}]> connected", this.socket.getRemoteSocketAddress());
            this.socketWriter = new PrintWriter(this.socket.getOutputStream());
            this.socketReader = new Scanner(this.socket.getInputStream());
        }

        void run() {
            threadPool.submit(this::sendSystemIn);
            accept();
            log("[{0}]> disconnected", this.socket.getRemoteSocketAddress());
            threadPool.shutdownNow();
            try {
                threadPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                // ignore
            }
        }

        private void sendSystemIn() {
            forever(System.in, line -> {
                socketWriter.println(line);
                if (socketWriter.checkError()) {
                    log("[{0}]> unable to write to socket", socket.getRemoteSocketAddress());
                }
            });
        }

        private void accept() {
            forever(socketReader, line -> log("[{0}]> says: {1}", socket.getRemoteSocketAddress(), line));
        }
    }

    private static class UdpServer {

        @Data
        @RequiredArgsConstructor
        private static class Client {
            private final int id;
            private final SocketAddress address;
            private Instant timestamp;
            @Override public String toString() { return id + " @ " + address; }
        }

        private final ExecutorService threadPool = Executors.newCachedThreadPool();
        private final Map<SocketAddress, Client> clients = new ConcurrentHashMap<>();
        private final DatagramSocket socket;
        private int clientSeq;
        private final boolean broadcasting;
        private final SocketAddress socketAddress;

        UdpServer(int port) throws IOException {
            this.broadcasting = false;
            this.clientSeq = 0;
            this.socket = new DatagramSocket(port);
            this.socketAddress = this.socket.getLocalSocketAddress();
        }

        UdpServer(String host, int port) throws IOException {
            this.broadcasting = true;
            this.clientSeq = 0;
            this.socket = new DatagramSocket();
            this.socket.setBroadcast(true);
            this.socketAddress = new InetSocketAddress(host, port);
        }

        void run() {
            threadPool.submit(this::broadcastSystemIn);
            threadPool.submit(() -> {
                while (true) {
                    try {
                        Thread.sleep(UDP_CLIENT_TIMEOUT.toMillis() / 4);
                    } catch (InterruptedException e) {
                        return;
                    }
                    disconnectStaleClients();
                }
            });
            while (true) {
                try {
                    accept();
                } catch (IOException e) {
                    log("exception while waiting for incoming connections: ", e.getMessage());
                    threadPool.shutdownNow();
                    try {
                        threadPool.awaitTermination(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ex) {
                        // ignore
                    }
                    return;
                }
            }
        }

        private void broadcastSystemIn() {
            forever(System.in, line -> {
                byte[] data = line.getBytes();
                if (broadcasting) {
                    try {
                        socket.send(new DatagramPacket(data, data.length, socketAddress));
                    } catch (IOException e) {
                        log("exception while broadcasting the message: {0}", e.getMessage());
                    }
                } else {
                    clients.values().forEach(c -> {
                        try {
                            socket.send(new DatagramPacket(data, data.length, c.getAddress()));
                        } catch (IOException e) {
                            log("exception while sending message to client [{0}]: {1}", c, e.getMessage());
                        }
                    });
                }
            });
        }

        private void accept() throws IOException {
            byte[] buf = new byte[DATAGRAM_PACKET_SIZE];
            DatagramPacket p = new DatagramPacket(buf, buf.length);
            socket.receive(p);
            ++clientSeq;
            handleConnection(p);
        }

        private void handleConnection(DatagramPacket p) {
            Client c = clients.compute(p.getSocketAddress(), (a, c0) -> {
                if (c0 == null) c0 = new Client(clientSeq, a);
                c0.setTimestamp(Instant.now());
                return c0;
            });
            handleClient(c, p);
        }

        private void handleClient(Client c, DatagramPacket p) {
            log("[{0}] says: {1}", c, new String(p.getData(), 0, p.getLength()));
        }

        private void disconnectStaleClients() {
            Instant now = Instant.now();
            for (Iterator<Client> it = clients.values().iterator(); it.hasNext();) {
                Client c = it.next();
                if (Duration.between(c.getTimestamp(), now).compareTo(UDP_CLIENT_TIMEOUT) > 0) {
                    it.remove();
                    log("[{0}] disconnected by timeout", c);
                }
            }
        }
    }

    private static class UdpClient {

        private final ExecutorService threadPool = Executors.newCachedThreadPool();
        private final DatagramSocket socket;
        private final SocketAddress server;

        UdpClient(String host, int port, boolean broadcasting) throws IOException {
            this.server = new InetSocketAddress(host, port);
            this.socket = new DatagramSocket(broadcasting ? port : 0);
            if (broadcasting) this.socket.setBroadcast(true);
            log("[{0}]> connected", this.server);
        }

        void run() {
            threadPool.submit(this::sendSystemIn);
            acceptAll();
            log("[{0}]> disconnected", this.socket.getRemoteSocketAddress());
            threadPool.shutdownNow();
            try {
                threadPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                // ignore
            }
        }

        private void sendSystemIn() {
            forever(System.in, line -> {
                byte[] data = line.getBytes();
                try {
                    socket.send(new DatagramPacket(data, data.length, server));
                } catch (IOException ex) {
                    log("[{0}]> exception while sending datagram: {1}", server, ex.getMessage());
                }
            });
        }

        private void acceptAll() {
            DatagramPacket p = new DatagramPacket(
                    new byte[DATAGRAM_PACKET_SIZE], DATAGRAM_PACKET_SIZE);
            while (true) {
                try {
                    socket.receive(p);
                    log("[{0}]> says: {1}", p.getSocketAddress(), new String(p.getData(), 0, p.getLength()));
                } catch (IOException ex) {
                    log("[{0}]> exception while receiving data: {1}", p.getSocketAddress(), ex.getMessage());
                    return;
                }
            }
        }
    }
}
