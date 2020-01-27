/*
 * IMDBLoader.java
 * 
 * Version: 
 *     $Id$ 
 * 
 * Revisions: 
 *     $Log$ 
 */
package IMDBLoader;

/**
 * @author Nikhil Kaushik (nk2214@rit.edu)
 */

public class IMDBLoader {

	public static void main(String[] args) {
		String url = args[0];
		String user = args[1];
		String password = args[2];
		String filePath = args[3];
		
		LoadAPI obj = new LoadAPI();
        obj.createSchema(url, user, password);
        obj.loadPersonData(url, user, password, filePath);
        obj.loadMovieData(url, user, password, filePath);
        obj.loadRatings(url, user, password, filePath);
        obj.loadTitlePrinicipals(url, user, password, filePath);

	}

} //IMDBLoader
