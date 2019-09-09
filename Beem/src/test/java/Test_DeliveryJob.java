import junit.framework.TestCase;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.InputMismatchException;

public class Test_DeliveryJob extends TestCase
{
    @Test
    public void testParseCity()
    {
        DeliveryJob job = new DeliveryJob(123456789, "Texas A&M University",
                "400 Bizzell St, College Station, TX 77843", "11410 Century Oaks Terrace, Austin, TX 78758",
                "Shelf", "My dad will help you load it up.", "Medium");

        String city = job.parseCity("400 Bizzell St, College Station, TX 77843");
        assertEquals("College Station", city);

        city = job.parseCity("11410 Century Oaks Terrace, Austin, TX 78758");
        assertEquals("Austin", city);
    }

    @Test
    public void testParseState()
    {
        DeliveryJob job = new DeliveryJob(123456789, "Texas A&M University",
                "400 Bizzell St, College Station, TX 77843", "11410 Century Oaks Terrace, Austin, TX 78758",
                "Shelf", "My dad will help you load it up.", "Medium");

        String state = job.parseState("400 Bizzell St, College Station, TX 77843");
        assertEquals("TX", state);

        state = job.parseState("1313 Disneyland Dr, Anaheim, CA 92802");
        assertEquals("CA", state);
    }

    @Test
    public void testParseZipCode()
    {
        DeliveryJob job = new DeliveryJob(123456789, "Texas A&M University",
                "400 Bizzell St, College Station, TX 77843", "11410 Century Oaks Terrace, Austin, TX 78758",
                "Shelf", "My dad will help you load it up.", "Medium");

        String zip = job.parseZipCode("400 Bizzell St, College Station, TX 77843");
        assertEquals("77843", zip);

        zip = job.parseZipCode("11313 Disneyland Dr, Anaheim, CA 92802");
        assertEquals("92802", zip);
    }

    @Test
    public void testItemSize()
    {
        DeliveryJob job = new DeliveryJob();

        int size = job.calcSize("medium");
        assertEquals(2, size);

        size = job.calcSize("small");
        assertEquals(1, size);

        size = job.calcSize("large");
        assertEquals(3, size);

        size = job.calcSize("MediuM");
        assertEquals(2, size);

        size = job.calcSize("SMALL");
        assertEquals(1, size);

        size = job.calcSize("lArGe");
        assertEquals(3, size);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testItemSizeExcep()
    {
        DeliveryJob job = new DeliveryJob();
        try
        {
            int size = job.calcSize("Extra Large");
            fail("Expected IllegalArgumentException");
        }
        catch(IllegalArgumentException e)
        {
            // Ignore. It's supposed to throw an exception. This means it passed.
        }
    }
}
