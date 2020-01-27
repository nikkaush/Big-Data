/*
 * LoadAPI.java
 * 
 * Version: 
 *     $Id$ 
 * 
 * Revisions: 
 *     $Log$ 
 */
package IMDBLoader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * @author Nikhil Kaushik (nk2214@rit.edu)
 */
public class LoadAPI {

	public void createSchema(String url, String user, String password) {
		List<String> queryList = new ArrayList<>();

		queryList.add("CREATE TABLE Person (" + "id INT(7), "
				+ "name VARCHAR(200), " + "birthYear INT(4), " + "deathYear INT(4), " + "PRIMARY KEY (id)" + ")");

		queryList.add("CREATE TABLE Movie (" + "id INT(7), " + "title VARCHAR(400), "
				+ "releaseYear INT(4), " + "runtime INT(10), " + "rating FLOAT(7, 5), " + "numberOfVotes INT(10), "
				+ "PRIMARY KEY(id))");

		queryList.add("CREATE TABLE ActedIn (" + "personId INT(7), " + "movieId INT(7), "
				+ "PRIMARY KEY (personId, movieId), " + "FOREIGN KEY (personId) REFERENCES Person(id), "
				+ "FOREIGN KEY (movieId) REFERENCES Movie(id))");

		queryList.add("CREATE TABLE ComposedBy (" + "personId INT(7), "
				+ "movieId INT(7), " + "PRIMARY KEY (personId, movieId), "
				+ "FOREIGN KEY (personId) REFERENCES Person(id), " + "FOREIGN KEY (movieId) REFERENCES Movie(id))");

		queryList.add("CREATE TABLE DirectedBy (" + "personId INT(7), "
				+ "movieId INT(7), " + "PRIMARY KEY (personId, movieId), "
				+ "FOREIGN KEY (personId) REFERENCES Person(id), " + "FOREIGN KEY (movieId) REFERENCES Movie(id))");

		queryList.add("CREATE TABLE EditedBy (" + "personId INT(7), "
				+ "movieId INT(7), " + "PRIMARY KEY (personId, movieId), "
				+ "FOREIGN KEY (personId) REFERENCES Person(id), " + "FOREIGN KEY (movieId) REFERENCES Movie(id))");

		queryList.add("CREATE TABLE ProducedBy (" + "personId INT(7), "
				+ "movieId INT(7), " + "PRIMARY KEY (personId, movieId), "
				+ "FOREIGN KEY (personId) REFERENCES Person(id), " + "FOREIGN KEY (movieId) REFERENCES Movie(id))");

		queryList.add("CREATE TABLE WrittenBy (" + "personId INT(7), "
				+ "movieId INT(7), " + "PRIMARY KEY (personId, movieId), "
				+ "FOREIGN KEY (personId) REFERENCES Person(id), " + "FOREIGN KEY (movieId) REFERENCES Movie(id))");

		queryList
				.add("CREATE TABLE Genre (" + "id INT(10), " + "name VARCHAR(20), " + "PRIMARY KEY(id))");

		queryList.add("CREATE TABLE HasGenre (" + "movieId INT(7), "
				+ "genreId INT(10), " + "PRIMARY KEY(movieId, genreId), "
				+ "FOREIGN KEY (movieId) REFERENCES Movie(id), " + "FOREIGN KEY (genreId) REFERENCES Genre(id))");
		
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, password);
			stmt = conn.createStatement();
			for (String query : queryList) {
				System.out.println(stmt.execute(query));
			}

		} catch (Exception e) {
			System.out.println("Exception from LoadAPI.createSchema-");
			e.printStackTrace();
		} finally {
			try{
				stmt.close();
				conn.close();
			} catch(Exception e){
				System.out.println("LoadAPI.createSchema: Exception occerred whie closing resources.");
			}
		}

	}

	public void loadPersonData(String url, String user, String password, String filePath) {
		
		Connection conn = null;
		PreparedStatement ps = null;
		BufferedReader br = null;
		InputStream fileStream = null;
		InputStream gzipStream = null;
		Reader decoder = null;


		String query = "INSERT INTO Person (id, name, birthYear, deathYear) VALUES (?, ?, ?, ?)";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, password);
			ps = conn.prepareStatement(query);
			fileStream = new FileInputStream(filePath + "name.basics.tsv.gz");
			gzipStream = new GZIPInputStream(fileStream);
			decoder = new InputStreamReader(gzipStream, "UTF-8");
			br = new BufferedReader(decoder);

			conn.setAutoCommit(false);
			int counter = 1;
			
			br.readLine();
			String s = null;
			while ((s = br.readLine()) != null) {
				counter++;
				String[] tempArr = s.split("\\t");

				ps.setInt(1, Integer.parseInt(tempArr[0].substring(2)));
				ps.setString(2, tempArr[1]);
				if (tempArr[2].equals("\\N"))
					ps.setNull(3, Types.INTEGER);
				else
					ps.setInt(3, Integer.parseInt(tempArr[2]));
				if (tempArr[3].equals("\\N"))
					ps.setNull(4, Types.INTEGER);
				else
					ps.setInt(4, Integer.parseInt(tempArr[3]));
				ps.addBatch();

				if (counter % 10000 == 0) {
					ps.executeBatch();
					conn.commit();
				}

			}
			ps.executeBatch();
			conn.commit();

		} catch (Exception e) {
			System.out.println("Exception from LoadAPI.loadPersonData-");
			e.printStackTrace();
		} finally {
			try {
				br.close();
				decoder.close();
				gzipStream.close();
				fileStream.close();
				ps.close();
				conn.close();
			} catch (Exception e) {
				System.out.println("LoadAPI.loadPersonData: Exception occured while Closing resources");
			}
		}

	}

	public void loadMovieData(String url, String user, String password, String filePath) {
		Connection conn = null;
		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		PreparedStatement ps3 = null;
		InputStream fileStream = null;
		InputStream gzipStream = null;
		Reader decoder = null;
		BufferedReader br = null;

		Set<String> genreSet = new HashSet<>();
		Map<String, Integer> genreMap = new HashMap<>();

		String query = "INSERT INTO Movie (id, title, releaseYear, runtime) VALUES (?, ?, ?, ?)";
		String genQuery = "INSERT INTO Genre(id, name) VALUES (?, ?)";
		String HasGenQuery = "INSERT INTO HasGenre (movieId, genreId) VALUES (?, ?)";
		try  {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, password);
			ps = conn.prepareStatement(query);
			ps2 = conn.prepareStatement(genQuery);
			ps3 = conn.prepareStatement(HasGenQuery);
			fileStream = new FileInputStream(filePath + "title.basics.tsv.gz");
			gzipStream = new GZIPInputStream(fileStream);
			decoder = new InputStreamReader(gzipStream, "UTF-8");
			br = new BufferedReader(decoder);
			
			conn.setAutoCommit(false);

			int counter = 1;
			int genCount = 1;

			br.readLine();
			String s = null;
			while ((s = br.readLine()) != null) {
				String[] tempArr = s.split("\\t");
				if (tempArr[1].equals("short") || tempArr[1].equals("tvShort") || tempArr[1].equals("movie")
						|| tempArr[1].equals("tvMovie")) {
					int movieId = Integer.parseInt(tempArr[0].substring(2));
					String[] tempGenArr = tempArr[8].split(",");
					for (String genre : tempGenArr) {
						if (genreSet.add(genre)) {
							ps2.setInt(1, genCount++);
							ps2.setString(2, genre);
							ps2.addBatch();
							genreMap.put(genre, genCount - 1);
						}
						ps3.setInt(1, movieId);
						ps3.setInt(2, genreMap.get(genre));
						ps3.addBatch();
					}

					ps.setInt(1, movieId);
					ps.setString(2, tempArr[3]);
					if (tempArr[5].equals("\\N"))
						ps.setNull(3, Types.INTEGER);
					else
						ps.setInt(3, Integer.parseInt(tempArr[5]));
					if (tempArr[7].equals("\\N"))
						ps.setNull(4, Types.INTEGER);
					else
						ps.setInt(4, Integer.parseInt(tempArr[7]));
					ps.addBatch();
				}

				if (counter % 10000 == 0) {
					ps.executeBatch();
					ps2.executeBatch();
					conn.commit();
					ps3.executeBatch();
					conn.commit();
				}
				counter++;
			}
			ps.executeBatch();
			ps2.executeBatch();
			conn.commit();
			ps3.executeBatch();
			conn.commit();

		} catch (Exception e) {
			System.out.println("Exception from LoadAPI.loadMovieData-");
			e.printStackTrace();
		} finally {
			try {
				br.close();
				decoder.close();
				gzipStream.close();
				fileStream.close();
				ps.close();
				ps2.close();
				ps3.close();
				conn.close();
			} catch (Exception e) {
				System.out.println("LoadAPI.loadMovieData: Exception occured while Closing resources");
			}
		}

	}

	public void loadRatings(String url, String user, String password, String filePath) {

		String query = "UPDATE Movie SET rating = ?, numberOfVotes = ? WHERE id = ?";

		Connection conn = null;
		PreparedStatement ps = null;
		BufferedReader br = null;
		InputStream fileStream = null;
		InputStream gzipStream = null;
		Reader decoder = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(query);
			fileStream = new FileInputStream(filePath + "title.ratings.tsv.gz");
			gzipStream = new GZIPInputStream(fileStream);
			decoder = new InputStreamReader(gzipStream, "UTF-8");
			br = new BufferedReader(decoder);

			int counter = 0;
			String s = null;

			br.readLine();
			while ((s = br.readLine()) != null) {
				String[] line = s.split("\\t");
				ps.setInt(3, Integer.parseInt(line[0].substring(2)));
				ps.setFloat(1, Float.parseFloat(line[1]));
				ps.setInt(2, Integer.parseInt(line[2]));
				ps.addBatch();
				counter++;

				if (counter % 10000 == 0) {
					ps.executeBatch();
					conn.commit();
				}

			}
			ps.executeBatch();
			conn.commit();

		} catch (Exception e) {
			System.out.println("Eception occured LoadAPI.loadRatings:");
			e.printStackTrace();

		} finally {
			try {
				br.close();
				decoder.close();
				gzipStream.close();
				fileStream.close();
				ps.close();
				conn.close();
			} catch (Exception e) {
				System.out.println("LoadAPI.loadRatings: Exception occured while Closing resources");
			}

		}

	}

	public void loadTitlePrinicipals(String url, String user, String password, String filePath) {

		List<String> categories = new ArrayList<>();
		categories.add("actor");
		categories.add("actress");
		categories.add("self");
		categories.add("composer");
		categories.add("director");
		categories.add("editor");
		categories.add("producer");
		categories.add("writer");

		String ActedInQuery = "INSERT IGNORE INTO ActedIn (personId, movieId) VALUES (?, ?)";
		String ComposedByQuery = "INSERT IGNORE INTO ComposedBy (personId, movieId) VALUES (?, ?)";
		String DirectedByQuery = "INSERT IGNORE INTO DirectedBy (personId, movieId) VALUES (?, ?)";
		String EditedByQuery = "INSERT IGNORE INTO EditedBy (personId, movieId) VALUES (?, ?)";
		String ProducedByQuery = "INSERT IGNORE INTO ProducedBy (personId, movieId) VALUES (?, ?)";
		String WrittenByQuery = "INSERT IGNORE INTO WrittenBy (personId, movieId) VALUES (?, ?)";
		
		Connection conn = null;
		PreparedStatement psActor = null;
		PreparedStatement psComposer = null;
		PreparedStatement psDirector = null;
		PreparedStatement psEditor = null;
		PreparedStatement psProducer = null;
		PreparedStatement psWriter = null;
		BufferedReader br = null;
		InputStream fileStream = null;
		InputStream gzipStream = null;
		Reader decoder = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, password);
			psActor = conn.prepareStatement(ActedInQuery);
			psComposer = conn.prepareStatement(ComposedByQuery);
			psDirector = conn.prepareStatement(DirectedByQuery);
			psEditor = conn.prepareStatement(EditedByQuery);
			psProducer = conn.prepareStatement(ProducedByQuery);
			psWriter = conn.prepareStatement(WrittenByQuery);
			
			fileStream = new FileInputStream(filePath + "title.principals.tsv.gz");
			gzipStream = new GZIPInputStream(fileStream);
			decoder = new InputStreamReader(gzipStream, "UTF-8");
			br = new BufferedReader(decoder);
			
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			ResultSet rs = null;

			boolean flag = true;
			int prevMovieId = -1;
			int counter = 0;

			br.readLine();
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] columns = line.split("\\t");
				String lineCategory = columns[3];
				int movieId = Integer.parseInt(columns[0].substring(2));
				if (prevMovieId == movieId) {
					if(flag){
						counter++;
						switch (lineCategory) {
						case "actor":
						case "actress":
						case "self":
							psActor.setInt(1, Integer.parseInt(columns[2].substring(2)));
							psActor.setInt(2, movieId);
							psActor.addBatch();
							break;
						case "director":
							psDirector.setInt(1, Integer.parseInt(columns[2].substring(2)));
							psDirector.setInt(2, movieId);
							psDirector.addBatch();
							break;
						
						case "writer":
							psWriter.setInt(1, Integer.parseInt(columns[2].substring(2)));
							psWriter.setInt(2, movieId);
							psWriter.addBatch();
							break;
							
						case "producer":
							psProducer.setInt(1, Integer.parseInt(columns[2].substring(2)));
							psProducer.setInt(2, movieId);
							psProducer.addBatch();
							break;
							
						case "composer":
							psComposer.setInt(1, Integer.parseInt(columns[2].substring(2)));
							psComposer.setInt(2, movieId);
							psComposer.addBatch();
							break;
							
						case "editor":
							psEditor.setInt(1, Integer.parseInt(columns[2].substring(2)));
							psEditor.setInt(2, movieId);
							psEditor.addBatch();
							break;
						}
					}
					
				} else {
					rs = stmt.executeQuery("select count(*) from Movie where id = " + movieId);
					prevMovieId = movieId;
					while (rs.next()) {
						if (rs.getInt(1) > 0) {
							counter++;
							flag = true;

							switch (lineCategory) {
							case "actor":
							case "actress":
							case "self":
								psActor.setInt(1, Integer.parseInt(columns[2].substring(2)));
								psActor.setInt(2, movieId);
								psActor.addBatch();
								break;
							case "director":
								psDirector.setInt(1, Integer.parseInt(columns[2].substring(2)));
								psDirector.setInt(2, movieId);
								psDirector.addBatch();
								break;
							
							case "writer":
								psWriter.setInt(1, Integer.parseInt(columns[2].substring(2)));
								psWriter.setInt(2, movieId);
								psWriter.addBatch();
								break;
								
							case "producer":
								psProducer.setInt(1, Integer.parseInt(columns[2].substring(2)));
								psProducer.setInt(2, movieId);
								psProducer.addBatch();
								break;
								
							case "composer":
								psComposer.setInt(1, Integer.parseInt(columns[2].substring(2)));
								psComposer.setInt(2, movieId);
								psComposer.addBatch();
								break;
								
							case "editor":
								psEditor.setInt(1, Integer.parseInt(columns[2].substring(2)));
								psEditor.setInt(2, movieId);
								psEditor.addBatch();
								break;
							}
						} else {
							flag = false;
						}
					}

				}

				if (counter % 10000 == 0) {
					psActor.executeBatch();
					psComposer.executeBatch();
					psDirector.executeBatch();
					psEditor.executeBatch();
					psProducer.executeBatch();
					psWriter.executeBatch();
					conn.commit();
					psActor.clearBatch();
					psComposer.clearBatch();
					psDirector.clearBatch();
					psEditor.clearBatch();
					psProducer.clearBatch();
					psWriter.clearBatch();
				}

			}
			psActor.executeBatch();
			psComposer.executeBatch();
			psDirector.executeBatch();
			psEditor.executeBatch();
			psProducer.executeBatch();
			psWriter.executeBatch();
			conn.commit();

		} catch (Exception e) {
			System.out.println("Exception from LoadAPI.loadTitlePrinicipals");
			e.printStackTrace();
		} finally {

			try {
				br.close();
				decoder.close();
				gzipStream.close();
				fileStream.close();
				decoder.close();
				gzipStream.close();
				fileStream.close();
				psWriter.close();
				psProducer.close();
				psEditor.close();
				psDirector.close();
				psComposer.close();
				psActor.close();
				conn.close();
				/*conn1.close();
				conn2.close();
				conn3.close();
				conn4.close();
				conn5.close();
				conn6.close();*/
			} catch (Exception e) {
			}

		}

	}

} //LoadAPI
