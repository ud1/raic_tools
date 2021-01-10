import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class Tester {

    static final String HOME = "/media/denis/tsbM/aicups/raic2020_codeCraft/versions/";
    static final String RUNNER_HOME = "/media/denis/tsbM/aicups/raic2020_codeCraft/docs/2020.12.21/";

    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach(consumer);
        }
    }

    static Future<Integer> startStrategy(String name, int port) throws Exception {
        CompletableFuture<Integer> result = new CompletableFuture();

        new Thread(() -> {

            try {
                ProcessBuilder builder = new ProcessBuilder();
                System.out.println("Start " + HOME + name + " 127.0.0.1 " + port);
                builder.command(HOME + name, "127.0.0.1", "" + port);
                builder.directory(new File(HOME));
                builder.redirectErrorStream(true);

                Process process = builder.start();

                new BufferedReader(new InputStreamReader(process.getInputStream())).lines()
                        .forEach(s -> {
                            //System.out.println(name + ": " + s);
                        });

                int exitCode = process.waitFor();
                result.complete(exitCode);
            }
            catch (Exception e)
            {
                result.completeExceptionally(e);
            }
        }).start();

        return result;
    }

    static Future<Integer> startRunner(String config, List<String> names) throws Exception {
        CompletableFuture<Integer> result = new CompletableFuture();

        new Thread(() -> {

            try {
                ProcessBuilder builder = new ProcessBuilder();
                List<String> cmds = new ArrayList<>();
                cmds.add(RUNNER_HOME + "aicup2020");
                cmds.add("--config");
                cmds.add(config);
                cmds.add("--save-results");
                cmds.add(config + "_res.json");
                cmds.add("--batch-mode");
                cmds.add("--player-names");
                cmds.addAll(names);

                builder.command(cmds);
                builder.directory(new File(RUNNER_HOME));
                builder.redirectErrorStream(true);

                Process process = builder.start();

                new BufferedReader(new InputStreamReader(process.getInputStream())).lines()
                        .forEach(s -> {
                            System.out.println("Runner " + config + ": " + s);
                        });

                int exitCode = process.waitFor();
                result.complete(exitCode);
            }
            catch (Exception e)
            {
                result.completeExceptionally(e);
            }
        }).start();

        return result;
    }

    static class Res
    {
        String name;
        int points;

        public Res(String name, int points) {
            this.name = name;
            this.points = points;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("Res{");
            sb.append("name='").append(name).append('\'');
            sb.append(", points=").append(points);
            sb.append('}');
            return sb.toString();
        }
    }

    static void saveResults(String config, List<String> strats, String type) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map res = objectMapper.readValue(new File(RUNNER_HOME + config + "_res.json"), Map.class);

        List<Integer> result = (List<Integer>) res.get("results");
        Long seed = ((Number)res.get("seed")).longValue();

        List<Res> results = new ArrayList<>();

        for (int i = 0; i < strats.size(); ++i)
        {
            results.add(new Res(strats.get(i), result.get(i)));
        }

        results.sort(Comparator.comparing(v -> -v.points));

        System.out.println("Results: " + results);

        saveToDb(seed, results, type);
    }

    static synchronized void saveToDb(long seed, List<Res> results, String type) throws Exception
    {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:LOCAL_TESTS.s3db")) {
            conn.setAutoCommit(false);

            Statement statmt = conn.createStatement();
            statmt.execute("CREATE TABLE if not exists 'game' ('id' INTEGER PRIMARY KEY AUTOINCREMENT, 'type' text, 'dt' TEXT, 'seed' INTEGER);");
            statmt.execute("CREATE TABLE if not exists 'game_result' ('id' INTEGER PRIMARY KEY AUTOINCREMENT, 'game_id' INTEGER, 'player_name' text, 'position' INTEGER, 'score' INTEGER, " +
                    "FOREIGN KEY(game_id) REFERENCES game(id));");

            PreparedStatement gameInsSt = conn.prepareStatement("INSERT INTO game(type, dt, seed) values (?, ?, ?)");
            PreparedStatement gameResInsSt = conn.prepareStatement("INSERT INTO game_result(game_id, player_name, position, score) values (?, ?, ?, ?)");

            gameInsSt.setString(1, type);
            gameInsSt.setString(2, LocalDateTime.now().toString());
            gameInsSt.setLong(3, seed);
            gameInsSt.executeUpdate();
            ResultSet rs = gameInsSt.getGeneratedKeys();
            rs.next();
            int id = rs.getInt(1);

            int i = 1;
            for (Res res : results)
            {
                gameResInsSt.setInt(1, id);
                gameResInsSt.setString(2, res.name);
                gameResInsSt.setInt(3, i);
                gameResInsSt.setInt(4, res.points);
                gameResInsSt.executeUpdate();

                ++i;
            }

            conn.commit();
        }
    }


    static CompletableFuture<Void> start(String config, int startPort, List<String> strats, String type) throws Exception
    {
        List<Future> futures = new ArrayList<>();

        futures.add(startRunner(config, strats));
        Thread.sleep(1000);

        for (String strat : strats)
        {
            futures.add(startStrategy(strat, startPort));
            startPort++;
        }

        CompletableFuture<Void>  res = CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{}));

        res = res.thenApply(r -> {
            try {
                saveResults(config, strats, type);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        return res;
    }

    public static void run(List<String> versionsList, String config, int startPort, String type)
    {
        try {
            ArrayList<String> versions = new ArrayList<>(versionsList);

            Collections.shuffle(versions);

            Future<Void> s = start(config, startPort, versions, type);
            s.get();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    static class ConfigParams
    {
        String config;
        String type;
        int startPort;

        public ConfigParams(String config, String type, int startPort) {
            this.config = config;
            this.type = type;
            this.startPort = startPort;
        }
    };

    static List<ConfigParams> R1 = Arrays.asList(
            new ConfigParams("config4_R1_1.json", "Round1", 41001),
            new ConfigParams("config4_R1_2.json", "Round1", 41011),
            new ConfigParams("config4_R1_3.json", "Round1", 41021),
            new ConfigParams("config4_R1_4.json", "Round1", 41031)
    );

    static List<ConfigParams> R2 = Arrays.asList(
            new ConfigParams("config4_R2_1.json", "Round2", 42001),
            new ConfigParams("config4_R2_2.json", "Round2", 42011),
            new ConfigParams("config4_R2_3.json", "Round2", 42021),
            new ConfigParams("config4_R2_4.json", "Round2", 42031)
    );

    static List<ConfigParams> F = Arrays.asList(
            new ConfigParams("config4_F1.json", "Finals", 43001),
            new ConfigParams("config4_F2.json", "Finals", 43011),
            new ConfigParams("config4_F3.json", "Finals", 43021),
            new ConfigParams("config4_F4.json", "Finals", 43031),
            new ConfigParams("config4_F5.json", "Finals", 43041),
            new ConfigParams("config4_F6.json", "Finals", 43051),
            new ConfigParams("config4_F7.json", "Finals", 43061),
            new ConfigParams("config4_F8.json", "Finals", 43071),
            new ConfigParams("config4_F9.json", "Finals", 43081),
            new ConfigParams("config4_F10.json", "Finals", 43091)
    );

    public static void main(String[] args) throws Exception {

        //List<String> versionsList = Arrays.asList("v17", "v21", "v22_5", "v17");
        //List<String> versionsList = Arrays.asList("v51_2", "v51_2", "v51_t", "v51_t");
        //List<String> versionsList = Arrays.asList("v35_7", "v36_2");
        //List<String> versionsList = Arrays.asList("v35_7", "v37");
        //List<String> versionsList = Arrays.asList("v38_2", "v37");
        List<String> versionsList = Arrays.asList("v51_2", "v52_3");

        List<ConfigParams> params = new ArrayList<>();
        //params.addAll(R2);
        //params.addAll(R1);
        params.addAll(F);


        int cnt = 40;

        List<Thread> threads = new ArrayList<>();

        for (ConfigParams p : params)
        {
            Thread t = new Thread(() -> {
                for (int i = 0; i < cnt; ++i) {
                    long start = System.currentTimeMillis();
                    run(versionsList, p.config, p.startPort, p.type);
                    long end = System.currentTimeMillis();
                    System.out.println("FIN TIME " + (end - start) / 1000.0);
                }

                System.out.println("FIN");
            });

            threads.add(t);

            t.start();
        }

        for (Thread t : threads)
        {
            t.join();
        }
    }
}
