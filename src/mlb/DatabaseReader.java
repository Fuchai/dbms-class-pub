package mlb;

/**
 * @author Roman Yasinovskyy
 */
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseReader {

    private Connection db_connection;
    private final String SQLITEDBPATH = "jdbc:sqlite:data/mlb.sqlite";

    public DatabaseReader() {
//        this.InitiateDatabase();
    }

    public void InitiateDatabase() {
        try {
            db_connection = DriverManager.getConnection(SQLITEDBPATH);

            // Read team json
//            System.out.println("readTeamFromJson");
            String jsonfilename = "data/mlb_teams.json";
            DatabaseWriter instance = new DatabaseWriter();
            ArrayList<Team> team_result = instance.readTeamFromJson(jsonfilename);
//            for (Team t:team_result){
//                String info = t.getName();
//                System.out.print("\n*********\n"+info);
//            }
            // address from text
//            System.out.println("readAddressFromTxt");
            String textfilename = "data/mlb_teams.txt";
            ArrayList<Address> address_result = instance.readAddressFromTxt(textfilename);
//            for (Address a:address_result){
//                System.out.print("\n*********\n"+a.getCity()+" "+a.getPhone()+" "+
//                        a.getSite()+" "+a.getState()+" "+a.getStreet()+" "+a.getUrl()+
//                        " "+a.getZip());

//            }
            // player from csv
//            System.out.println("readPlayerFromCsv");
            String csvfilename = "data/mlb_players.csv";
            ArrayList<Player> player_result = instance.readPlayerFromCsv(csvfilename);
//            for (Player p:player_result){
//                System.out.print("\n*********\n"+p.getId()+" "+p.getPosition()+" "+
//                        p.getName()+" "+p.getTeam());
//            }

//            System.out.println("createTables");
            String db_filename = "mlb.sqlite";
            // create tables
            instance.createTables(db_filename);
            // insert all
            instance.writeTeamTable(db_filename, team_result);
            // address.team NOT NULL constraint failed.
            instance.writeAddressTable(db_filename, address_result);
            // player.team null constraint failed
            instance.writePlayerTable(db_filename, player_result);
        } catch (SQLException ex) {
            System.out.print("initiation error");
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Connect to a database (file)
     */
    public static void main(String args[]) {
        DatabaseReader a = new DatabaseReader();
        a.getTeamInfo("Chicago Cubs");
    }

    public void connect() {
        try {
            this.db_connection = DriverManager.getConnection(SQLITEDBPATH);
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReaderGUI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Disconnect from a database (file)
     */
    public void disconnect() {
        try {
            this.db_connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReaderGUI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Populate the list of divisions
     *
     * @param divisions
     */
    public void getDivisions(ArrayList<String> divisions) {
        Statement stat;
        ResultSet rs;

        DatabaseReader a = new DatabaseReader();
        a.connect();
        try {
            stat = a.db_connection.createStatement();
            // TODO: Write an SQL statement to retrieve a league (conference) and a division
            String sql = "SELECT DISTINCT division,conference FROM team";
            rs = stat.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                String divisionstring = " | ";
                for (int i = 1; i <= columnsNumber; i++) {
                    divisionstring += rs.getString(i);
                }
                divisions.add(divisionstring);
            }
//            System.out.print("printing division arrays:");
//            System.out.print(divisions);
            rs.close();
            // TODO: Add all 6 combinations to the ArrayList divisions

        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            a.disconnect();
        }
    }

    /**
     * Read all teams from the database
     *
     * @param confDiv
     * @param teams
     */
    public void getTeams(String confDiv, ArrayList<String> teams) {
        Statement stat;
        ResultSet results;
        String conference = confDiv.split(" | ")[0];
        String division = confDiv.split(" | ")[2];

        this.connect();
        try {
            stat = this.db_connection.createStatement();
            // TODO: Write an SQL statement to retrieve a teams from a specific division
            String[] conf_divi = confDiv.split("[|]");
            conf_divi[0] = conf_divi[0].replaceAll("\\s+", "");
            conf_divi[1] = conf_divi[1].replaceAll("\\s+", "");
//            System.out.print(conf_divi[0] + "heyo" + conf_divi[1]);
            String sql = "SELECT * FROM team WHERE conference==\"" + conf_divi[0] + "\"and division==\"" + conf_divi[1] + "\"";

//            System.out.print("sqlcommand:" + sql + "\n");
            results = stat.executeQuery(sql);
            // TODO: Add all 5 teams to the ArrayList teams
            ResultSetMetaData rsmd = results.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (results.next()) {
                String teamString = "";
                for (int i = 1; i <= columnsNumber; i++) {
                    teamString += results.getString(i);
                }
                teams.add(teamString);
            }
//            System.out.print("printing team arrays:");
//            System.out.print(teams);
            results.close();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            this.disconnect();
        }
    }

    /**
     * @param teamName
     * @return Team info
     */
    public Team getTeamInfo(String teamName) {
        Team team = null;
        // TODO: Retrieve team info (roster, address, and logo) from the database
        Statement stat;
        this.connect();
        try {
            stat = this.db_connection.createStatement();
            String team_idpk_SQL = "SELECT * FROM team WHERE name==\"" + teamName + "\"";
            // get team
            ResultSet teamResults;
            teamResults = stat.executeQuery(team_idpk_SQL);
            ResultSetMetaData idpk_rsmd = teamResults.getMetaData();
            int teamColumnsNumber = idpk_rsmd.getColumnCount();
            ArrayList<String> teamInfo = new ArrayList<>();
            while (teamResults.next()) {
                for (int i = 1; i <= teamColumnsNumber; i++) {
                    teamInfo.add(teamResults.getString(i));
                }
            }
//            System.out.print("teamInfo+" + teamInfo);

            Statement stat2;
            stat2 = this.db_connection.createStatement();

            String roster_sql = "SELECT * FROM player WHERE team==\"" + teamInfo.get(0) + "\"";
            // iterate over all players for roster
            ResultSet playerResults;
            playerResults = stat2.executeQuery(roster_sql);
            ResultSetMetaData player_rsmd = playerResults.getMetaData();
            int playerColumnsNumber = player_rsmd.getColumnCount();
            ArrayList<Player> roster = new ArrayList<>();
            while (playerResults.next()) {
                // get a single player's info
                ArrayList<String> fields = new ArrayList<>();
                for (int i = 1; i <= playerColumnsNumber; i++) {
                    fields.add(playerResults.getString(i));
                }
//                System.out.print("fields:" + fields);
                Player singlePlayer = new Player(fields.get(1), fields.get(2), teamName, fields.get(4));
                roster.add(singlePlayer);
//                System.out.print(singlePlayer+"\n");
            }

            // get address for the team
            String address_sql = "SELECT * FROM address WHERE team==\"" + teamInfo.get(0) + "\"";
            // iterate over all players for roster
            ResultSet addressResults;
            addressResults = stat2.executeQuery(address_sql);
            Address teamAddress = new Address(teamName,
                    addressResults.getString("site"), addressResults.getString("street"),
                    addressResults.getString("city"), addressResults.getString("state"),
                    addressResults.getString("zip"), addressResults.getString("phone"),
                    addressResults.getString("url"));

            // convert teamInfo.get(7) from blob

//            int blobLength = (int) blob.length();
//            byte[] blobAsBytes = blob.getBytes(1, blobLength);
            team = new Team(teamInfo.get(1), teamInfo.get(2), teamInfo.get(3), teamInfo.get(4), teamInfo.get(5));
            team.setAddress(teamAddress);
            team.setRoster(roster);
            try{
                byte[] blob = teamResults.getBytes("logo");
                team.setLogo(blob);

            }catch (SQLException ex){
//                System.out.print("no blob yo?");
            }

        } catch (SQLException ex) {
            Logger.getLogger(DatabaseReader.class.getName()).log(Level.SEVERE, null, ex);
            System.out.print(ex);

        }
        ResultSet results;

        return team;
    }

    private void assertEquals(int expResult, int size) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void assertTrue(boolean b) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
