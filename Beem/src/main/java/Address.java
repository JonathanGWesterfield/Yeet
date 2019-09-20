import java.util.IllegalFormatException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
/**
 * Since we need to parse and verify addresses a lot, this class will hold all the functions to avoid
 * repeating code for the DeliveryJob and RideShareJob classes.
 *
 * @author Jonathan Westerfield
 * @version 1.0
 * @since 9/17/2019
 *
 * @see DeliveryJob
 * @see RideShareJob
 * @see "https://stackoverflow.com/questions/7780981/how-to-validate-latitude-and-longitude"
 */
public class Address
{
    private static HashMap<String, String> states;
    static {
        states = new HashMap<>();
        states.put("AL", "ALABAMA");
        states.put("AK", "ALASKA");
        states.put("AZ", "ARIZONA");
        states.put("AR", "ARKANSAS");
        states.put("CA", "CALIFORNIA");
        states.put("CO", "COLORADO");
        states.put("CT", "CONNECTICUT");
        states.put("DE", "DELAWARE");
        states.put("FL", "FLORIDA");
        states.put("GA", "GEORGIA");
        states.put("HI", "HAWAII");
        states.put("ID", "IDAHO");
        states.put("IL", "ILLINOIS");
        states.put("IN", "INDIANA");
        states.put("IA", "IOWA");
        states.put("KS", "KENTUCKY");
        states.put("LA", "LOUISIANA");
        states.put("ME", "MAINE");
        states.put("MD", "MARYLAND");
        states.put("MA", "MASSACHUSETTS");
        states.put("MI", "MICHIGAN");
        states.put("MN", "MINNESOTA");
        states.put("MS", "MISSISSIPPI");
        states.put("MO", "MISSOURI");
        states.put("MT", "MONTANA");
        states.put("NE", "NEBRASKA");
        states.put("NV", "NEVADA");
        states.put("NH", "NEW HAMPSHIRE");
        states.put("NJ", "NEW JERSEY");
        states.put("NM", "NEW MEXICO");
        states.put("NY", "NEW YORK");
        states.put("NC", "NORTH CAROLINA");
        states.put("ND", "NORTH DAKOTA");
        states.put("OH", "OHIO");
        states.put("OK", "OKLAHOMA");
        states.put("OR", "OREGON");
        states.put("PA", "PENNSYLVANIA");
        states.put("RI", "RHODE ISLAND");
        states.put("SC", "SOUTH CAROLINA");
        states.put("SD", "SOUTH DAKOTA");
        states.put("TN", "TENNESSEE");
        states.put("TX", "TEXAS");
        states.put("UT", "UTAH");
        states.put("VT", "VERMONT");
        states.put("VA", "VIRGINIA");
        states.put("WA", "WASHINGTON");
        states.put("WV", "WEST VIRGINIA");
        states.put("WI", "WISCONSIN");
        states.put("WY", "WYOMING ");
    }
    /**
     * Parses the address and returns the city in the Address. Uses the comma in the address as the delimiter.
     * @param address The address we need to parse.
     * @return The city in the address. Example is 'Houston'.
     */
    public static String parseCity(String address)
    {
        StringTokenizer tokenizer = new StringTokenizer(address, ",");
        tokenizer.nextToken();
        String city = tokenizer.nextToken();

        return city.trim();
    }

    /**
     * Parses the address and returns the state in the Address. Uses the comma in the address as the delimiter.
     * @param address The address we need to parse the state out of.
     * @return The state in the address. Example is 'TX'.
     */
    public static String parseState(String address)
    {
        String[] addyArr = address.split(" ");
        return addyArr[addyArr.length - 2];
    }

    /**
     * Parses the address and return the zip code in the address. Uses the comma in the address as the delimiter.
     * @param address The address we need to parse the zip code from.
     * @return The zip code in the address. Example is '77064'
     * @throws IllegalArgumentException Throws if the zip code is not a valid U.S. postal format
     */
    public static String parseZipCode(String address)
    {
        String[] addyArr = address.split(" ");

        if (isValidZipCode(addyArr[addyArr.length - 1]))
            return addyArr[addyArr.length - 1];

        throw new IllegalArgumentException("Zip code is not correct format!");
    }

    /**
     * Validates whether or not the zip code provided is a valid us postal zip code.
     * @param zipCode The zipcode we need to validate.
     * @return True if valid, false otherwise.
     */
    public static boolean isValidZipCode(String zipCode)
    {
        String regex = "^[0-9]{5}(?:-[0-9]{4})?$";
        Pattern pattern = Pattern.compile(regex);

        Matcher matcher = pattern.matcher(zipCode);
        return matcher.matches();
    }

    /**
     * Verifies whether or not the latitude provided is valid. Latitude coordinates must be a double between -90
     * and +90.
     * @see "https://stackoverflow.com/questions/7780981/how-to-validate-latitude-and-longitude"
     * @param latitude The latitude coordinate we need to verify.
     * @return True if the coordinate is valid, false otherwise.
     */
    public static boolean isValidLatitude(double latitude)
    {
        if (latitude > -90.0 && latitude < 90.0)
            return true;
        return false;
    }

    /**
     * Verifies whether or not the longitude provided is valid. Longitude coordinates must be a double
     * between -180 and +180.
     * @see "https://stackoverflow.com/questions/7780981/how-to-validate-latitude-and-longitude"
     * @param longitude The longitude coordinate we need to verify.
     * @return True if the coordinate is valid, false otherwise
     */
    public static boolean isValidLongitude(double longitude)
    {
        if(longitude > -180.0 && longitude < 180)
            return true;
        return false;
    }

    /**
     * Checks to make sure that the state code we are trying to use is actually valid. Is also used to enforce
     * using the 2 character state codes for every state instead of using the full state name as to avoid spelling
     * errors and enforce consistency.
     * @param stateCode The state code. Ex: TX, CA
     * @return True if the state code is valid, false otherwise.
     */
    public static boolean isValidState(String stateCode)
    {
        if(states.containsKey(stateCode))
            return true;
        return false;
    }

}
