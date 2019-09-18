import junit.framework.TestCase;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.InputMismatchException;

public class Test_Address 
{
    @Test
    public void testParseCity()
    {
        String city = Address.parseCity("400 Bizzell St, College Station, TX 77843");
        assertEquals("College Station", city);

        city = Address.parseCity("11410 Century Oaks Terrace, Austin, TX 78758");
        assertEquals("Austin", city);

        System.out.println("parseCity() passed!");
    }

    @Test
    public void testParseState()
    {
        String state = Address.parseState("400 Bizzell St, College Station, TX 77843");
        assertEquals("TX", state);

        state = Address.parseState("1313 Disneyland Dr, Anaheim, CA 92802");
        assertEquals("CA", state);

        System.out.println("parseState() passed!");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testParseZipCode()
    {
        String zip = Address.parseZipCode("400 Bizzell St, College Station, TX 77843");
        assertEquals("77843", zip);

        zip = Address.parseZipCode("11313 Disneyland Dr, Anaheim, CA 92802");
        assertEquals("92802", zip);

        zip = Address.parseZipCode("11313 Disneyland Dr, Anaheim, CA 928028");

        System.out.println("parseZipCode() passed!");
    }

    @Test
    public void testIsValidZipCode()
    {
        String zip1 = "12345", zip2 = "12345-6789", invZip1 = "123456", invZip2 = "1234";
        String invZip3 = "12345-678", invZip4 = "12345-67890";

        assertEquals(true, Address.isValidZipCode(zip1));
        assertEquals(true, Address.isValidZipCode(zip2));

        assertEquals(false, Address.isValidZipCode(invZip1));
        assertEquals(false, Address.isValidZipCode(invZip2));
        assertEquals(false, Address.isValidZipCode(invZip3));
        assertEquals(false, Address.isValidZipCode(invZip4));

        System.out.println("isValidZipCode() passed!");
    }

    @Test
    public void testIsValidLatitude()
    {
        double lat1 = 30.616249, lat2 = -89.99999, lat3 = 89.999999, invlat1 = -90.0, invlat2 = 90.0, invlat3 = 43215;
        assertEquals(true, Address.isValidLatitude(lat1));
        assertEquals(true, Address.isValidLatitude(lat2));
        assertEquals(true, Address.isValidLatitude(lat3));

        assertEquals(false, Address.isValidLatitude(invlat1));
        assertEquals(false, Address.isValidLatitude(invlat2));
        assertEquals(false, Address.isValidLatitude(invlat3));

        System.out.println("isValidLatitude() passed!");
    }

    @Test
    public void testIsValidLongitude()
    {
        double long1 = -96.336853, long2 = -179.99999, long3 = 179.99999;
        double invlong1 = -180.0, invlong2 = 180, invlong3 = 32432;

        assertEquals(true, Address.isValidLongitude(long1));
        assertEquals(true, Address.isValidLongitude(long2));
        assertEquals(true, Address.isValidLongitude(long3));

        assertEquals(false, Address.isValidLongitude(invlong1));
        assertEquals(false, Address.isValidLongitude(invlong2));
        assertEquals(false, Address.isValidLongitude(invlong3));

        System.out.println("isValidLongitude() passed!");

    }
    
}
