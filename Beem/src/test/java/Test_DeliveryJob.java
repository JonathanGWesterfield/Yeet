import junit.framework.TestCase;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.InputMismatchException;

public class Test_DeliveryJob extends TestCase
{
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

        size = job.calcSize(DeliveryJob.Sizes.SMALL);
        assertEquals(1, size);

        size = job.calcSize(DeliveryJob.Sizes.MEDIUM);
        assertEquals(2, size);

        size = job.calcSize(DeliveryJob.Sizes.LARGE);
        assertEquals(3, size);

        System.out.println("calcSize() passed!");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testItemSizeExcept()
    {
        DeliveryJob job = new DeliveryJob();
        try
        {
            int size = job.calcSize("Extra Large");
            fail("Expected IllegalArgumentException");
        }
        catch(IllegalArgumentException e)
        {
            System.out.println("Item Size threw the correct Exception");
            // Ignore. It's supposed to throw an exception. This means it passed.
        }
    }

    @Test
    public void testEquals()
    {
        DeliveryJob job = new DeliveryJob(123456789, "Texas A&M University",
                "400 Bizzell St, College Station, TX 77843", "11410 Century Oaks Terrace, Austin, TX 78758",
                "Shelf", "My dad will help you load it up.", DeliveryJob.Sizes.MEDIUM);

        DeliveryJob sameJob = new DeliveryJob(123456789, "Texas A&M University",
                "400 Bizzell St, College Station, TX 77843", "11410 Century Oaks Terrace, Austin, TX 78758",
                "Shelf", "My dad will help you load it up.", DeliveryJob.Sizes.MEDIUM);

        DeliveryJob diffJob = new DeliveryJob(123456789, "Texas A&M University",
                "400 Bizzell St, College Station, TX 77843", "11410 Century Oaks Terrace, Austin, TX 78758",
                "Shelf", "My dad will help you load it up.", DeliveryJob.Sizes.LARGE);

        // Test if 2 same jobs with different references are deemed equal
        assertEquals(true, job.equals(sameJob));

        // Test if 2 different jobs are different
        assertEquals(false, job.equals(diffJob));

        // Test if 2 different jobs are different
        assertEquals(false, sameJob.equals(diffJob));

        // Test if job equals itself
        assertEquals(true, job.equals(job));

        System.out.println("equals() passed!");
    }
}
