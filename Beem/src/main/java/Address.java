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
}
