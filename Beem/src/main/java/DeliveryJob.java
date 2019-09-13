import java.io.Serializable;
import java.util.Date;
import java.util.InputMismatchException;
import java.util.StringTokenizer;

public class DeliveryJob implements Serializable
{
    private String job_type;
    private long customer_id;
    private String school;
    private String to_address;
    private String to_city;
    private String to_state;
    private String to_zipcode;
    private String from_address;
    private String from_city;
    private String from_state;
    private String from_zipcode;
    private String description;
    private String instructions;
    private int item_size;
    private Date pickup_time;

    /**
     * Default Empty Constructor
     */
    public DeliveryJob()
    {
        /* Default Empty Constructor */
    }

    /**
     * Constructor for a delivery job.
     * @param cust_id Customer ID in the database.
     * @param school What school the customer goes to. Ex: 'Texas A&M University'.
     * @param to_address What address driver needs to deliver package to.
     * @param from_address What address the driver needs to get the package from.
     * @param description Description of the package the driver needs to deliver.
     * @param instructions The pickup, drop-off, and handling instructions for the package being delivered.
     * @param item_size The size of the package to be delivered. Valid options are 'small', 'medium', and 'large'.
     */
    public DeliveryJob(int cust_id, String school, String to_address, String from_address, String description,
                           String instructions, String item_size)
    {
        this.customer_id = cust_id;
        this.school = school;
        this.to_address = to_address;
        this.to_city = parseCity(to_address);
        this.to_state = parseState(to_address);
        this.to_zipcode = parseZipCode(to_address);
        this.from_address = from_address;
        this.from_city = parseCity(from_address);
        this.from_state = parseState(from_address);
        this.from_zipcode = parseZipCode(from_address);
        this.description = description;
        this.instructions = instructions;
        this.item_size = calcSize(item_size);
    }

    /** Getters*/

    public String getJob_type()
    {
        return this.job_type;
    }

    public long getCustomer_ID()
    {
        return this.customer_id;
    }

    public String getSchool()
    {
        return this.school;
    }

    public String getTo_address()
    {
        return this.to_address;
    }

    public String getTo_city()
    {
        return this.to_city;
    }

    public String getTo_state()
    {
        return this.to_state;
    }

    public String getTo_zipcode()
    {
        return this.to_zipcode;
    }

    public String getFrom_address()
    {
        return this.from_address;
    }

    public String getFrom_city()
    {
        return this.from_city;
    }

    public String getFrom_state()
    {
        return this.from_state;
    }

    public String getFrom_zipcode()
    {
        return this.from_zipcode;
    }

    public String getDescription()
    {
        return this.description;
    }

    public String getInstructions()
    {
        return this.instructions;
    }

    /**
     * Parses the address and returns the city in the Address. Uses the comma in the address as the delimiter.
     * @param address The address we need to parse.
     * @return The city in the address. Example is 'Houston'.
     */
    public String parseCity(String address)
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
    public String parseState(String address)
    {
        String[] addyArr = address.split(" ");
        return addyArr[addyArr.length - 2];
    }

    /**
     * Parses the address and return the zip code in the address. Uses the comma in the address as the delimiter.
     * @param address The address we need to parse the zip code from.
     * @return The zip code in the address. Example is '77064'
     */
    public String parseZipCode(String address)
    {
        String[] addyArr = address.split(" ");
        return addyArr[addyArr.length - 1];
    }

    /**
     * Determines what integer represents the item size. 'small' size = 1, 'medium' = 2, and 'large' = 3.
     * @param itemSize The size of the item. Valid sizes are 'small', 'medium' and 'large'
     * @throws InputMismatchException Will throw an exception if a size that isn't 'small', 'medium', or 'large' is passed in.
     * @return The integer to represent the size of the object.
     */
    public int calcSize(String itemSize)
    {
        String size = itemSize.toLowerCase();
        if (size.equals("small"))
            return 1;
        else if (size.equals("medium"))
            return 2;
        else if (size.equals("large"))
            return 3;
        else
            throw new IllegalArgumentException("Invalid item size. Sizes must be either 'small', 'medium' or 'large'!");
    }

}

















