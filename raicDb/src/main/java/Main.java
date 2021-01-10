import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Main {
    public static void main(String[] args) throws Exception
    {
        Class.forName("org.sqlite.JDBC");

        ObjectMapper objectMapper = new ObjectMapper();

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:SAND.s3db"))
        {
            conn.setAutoCommit(false);

            Statement statmt = conn.createStatement();
            statmt.execute("CREATE TABLE if not exists 'game' ('id' INTEGER PRIMARY KEY, 'type' text);");
            statmt.execute("CREATE TABLE if not exists 'game_result' ('id' INTEGER PRIMARY KEY AUTOINCREMENT, 'game_id' INTEGER, 'player_name' text, 'player_version' INTEGER, 'position' INTEGER, 'score' INTEGER, " +
                    "FOREIGN KEY(game_id) REFERENCES game(id));");


            PreparedStatement gameInsSt = conn.prepareStatement("INSERT INTO game(id, type) values (?, ?)");
            PreparedStatement gameResInsSt = conn.prepareStatement("INSERT INTO game_result(game_id, player_name, player_version, position, score) values (?, ?, ?, ?, ?)");


            try (BufferedReader reader = new BufferedReader(new FileReader("/media/denis/tsbM/aicups/trueskill/raic2020/download/R.json"))) {
                String line = reader.readLine();

                int lines = 0;
                while (line != null) {
                    ++lines;
                    System.out.println("Process " + lines);

                    if (!line.isEmpty()) {
                        Map v = objectMapper.readValue(line, Map.class);

                        int num = Integer.parseInt((String) v.get("num"));
                        String type = (String) v.get("type");

                        ResultSet resSet = statmt.executeQuery("SELECT * FROM game WHERE id = " + num);
                        if (!resSet.next())
                        {
                            gameInsSt.setInt(1, num);
                            gameInsSt.setString(2, type);
                            gameInsSt.executeUpdate();

                            List<String> players = (List<String>) v.get("p");
                            List<Integer> versions = (List<Integer>) v.get("v");
                            List<Integer> points = (List<Integer>) v.get("pts");

                            for (int i = 0; i < players.size(); ++i)
                            {
                                if (players.get(i) != null && !players.get(i).isEmpty()) {
                                    gameResInsSt.setInt(1, num);
                                    gameResInsSt.setString(2, players.get(i));
                                    gameResInsSt.setInt(3, versions.get(i));
                                    gameResInsSt.setInt(4, i + 1);
                                    gameResInsSt.setInt(5, points.get(i));
                                    gameResInsSt.executeUpdate();
                                }
                            }
                        }
                    }

                    line = reader.readLine();
                }
            }

            conn.commit();

        }

    }
}
