//import sun.security.provider.AbstractDrbg;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

/**
 * This class represents the Rideshare jobs that customers can also request from the system. I'm having it as separate
 * object from DeliveryJob to enforce that these are 2 different objects with different data and different
 * responsibilities. Also, I don't use camel case for the class members since the names are used as identification
 * in the JSON object that gets sent into kafka. It's in keeping with making the JSON readable.
 *
 * @author Jonathan Westerfield
 * @version 1.0
 * @since 9/17/2019
 *
 */
public class RideShareJob implements Serializable
{
    private String job_type;
    private long customer_id;
    private String school;
    private double to_latitude;
    private double to_longitude;
    private String to_address;
    private String to_city;
    private String to_state;
    private String to_zipcode;
    private double from_latitude;
    private double from_longitude;
    private String from_address;
    private String from_city;
    private String from_state;
    private String from_zipcode;
    private int num_passengers;
    private Date pickup_time;
    private Date dropoff_time;

    /**
     * Default Constructor.
     */
    public RideShareJob() { /* Default Constructor */}

    /**
     * Constructor for the class. Allows you to only need to pass customer id, school, to and from address and number
     * of passengers. You will need to create the latitude and longitude of both addresses later if you use
     * this constructor.
     * @param customer_id Customer ID in the database.
     * @param school What school the customer goes to. Ex: 'Texas A&M University'.
     * @param to_address What address the passenger(s) need to get to.
     * @param from_address What address the passenger(s) need to get picked up from.
     * @param num_passengers Number of passengers for this ride.
     */
    public RideShareJob(long customer_id, String school, String to_address, String from_address, int num_passengers)
    {
        this.job_type = "RideShare";
        this.customer_id = customer_id;
        this.school = school;
        this.to_address = to_address;
        this.to_city = Address.parseCity(to_address);
        this.to_state = Address.parseState(to_address);
        this.to_zipcode = Address.parseZipCode(to_address);
        this.from_address = from_address;
        this.from_city = Address.parseCity(from_address);
        this.from_state = Address.parseState(from_address);
        this.from_zipcode = Address.parseZipCode(from_address);
        this.num_passengers = num_passengers;
    }

    /**
     * Constructor for the class. Similar to the simpler constructor but allows to specifiy the to and from coordinates
     * so we don't need to do a second lookup on the address for them. Should allow to easily send to map app and
     * go from there.
     *
     * @param customer_id Customer ID in the database.
     * @param school What school the customer goes to. Ex: 'Texas A&M University'.
     * @param to_latitude 
     * @param to_longitude
     * @param to_address What address the passenger(s) need to get to.
     * @param from_latitude
     * @param from_longitude
     * @param from_address What address the passenger(s) need to get picked up from.
     * @param num_passengers Number of passengers for this ride.
     */
    public RideShareJob(long customer_id, String school, double to_latitude, double to_longitude, String to_address,
                        double from_latitude, double from_longitude,String from_address, int num_passengers)
    {
        this.job_type = "RideShare";
        this.customer_id = customer_id;
        this.school = school;
        setTo_latitude(to_latitude);
        setTo_longitude(to_longitude);
        this.to_address = to_address;
        this.to_city = Address.parseCity(to_address);
        this.to_state = Address.parseState(to_address);
        this.to_zipcode = Address.parseZipCode(to_address);
        setFrom_latitude(from_latitude);
        setFrom_longitude(from_longitude);
        this.from_address = from_address;
        this.from_city = Address.parseCity(from_address);
        this.from_state = Address.parseState(from_address);
        this.from_zipcode = Address.parseZipCode(from_address);
        this.num_passengers = num_passengers;

    }

    /* Getters */
    /** Getters*/

    public String getJob_type()
    {
        return this.job_type;
    }

    public long getCustomer_id()
    {
        return this.customer_id;
    }

    public String getSchool()
    {
        return this.school;
    }

    public double getTo_latitude()
    {
        return this.to_latitude;
    }

    public double getTo_longitude()
    {
        return this.to_longitude;
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

    public double getFrom_latitude()
    {
        return this.from_latitude;
    }

    public double getFrom_longitude()
    {
        return this.from_longitude;
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

    public int getNum_passengers()
    {
        return this.num_passengers;
    }

    public Date getPickup_time()
    {
        return this.pickup_time;
    }

    public Date getDropoff_time()
    {
        return this.dropoff_time;
    }


    /** Setters */
    public void setCustomer_id(long id)
    {
        this.customer_id = id;
    }

    /**
     * Sets the school the customer is currently attending. Ex: 'Texas A&M University'.
     * @param school The school the customer is attending.
     */
    public void setSchool(String school)
    {
        this.school = school;
    }

    /**
     * Setter for the to_address member. Also sets the to_city, to_state, and to_zipcode members since they
     * are all derived from to_address. Address must follow a strict format of
     * "<Room number, PO BOX, etc>, <City>, <State> <Zip Code>"
     * @param address The address we need to send the package to. Example: "11410 Century Oaks Terrace, Austin, TX 78758"
     */
    public void setTo_address(String address)
    {
        this.to_address = address;
        this.to_city = Address.parseCity(address);
        this.to_state = Address.parseState(address);
        this.to_zipcode = Address.parseZipCode(address);
    }

    /**
     * Setter for the from_address member. Also sets the from_city, from_state, and from_zipcode members since they
     * are all derived from from_address. Address must follow a strict format of
     *      * "<Room number, PO BOX, etc>, <City>, <State> <Zip Code>"
     * @param address The address we are picking the package up from. Example: "11410 Century Oaks Terrace, Austin, TX 78758"
     */
    public void setFrom_address(String address)
    {
        this.from_address = address;
        this.from_city = Address.parseCity(address);
        this.from_state = Address.parseState(address);
        this.from_zipcode = Address.parseZipCode(address);
    }

    /**
     * Setter for the to_latitude member. Throws an exception if the latitude coordinates are not valid.
     * @param latitude The latitude coordinate we want to validate and set
     * @throws IllegalArgumentException Gets thrown if coordinate is not valid.
     */
    public void setTo_latitude(double latitude)
    {
        if (Address.isValidLatitude(latitude))
            this.to_latitude = latitude;
        else
            throw new IllegalArgumentException("Latitude coordinates must be > -90 and < 90!");
    }

    /**
     * Setter for the to_longitude member. Throws an exception if the longitude coordinates are not valid.
     * @param longitude The longitude coordinate we want to validate and set
     * @throws IllegalArgumentException Gets thrown if coordinate is not valid.
     */
    public void setTo_longitude(double longitude)
    {
        if (Address.isValidLongitude(longitude))
            this.to_longitude = longitude;
        else
            throw new IllegalArgumentException("Longitude coordinates must be > -180 and < 180");
    }

    /**
     * Setter for the from_latitude member. Throws an exception if the latitude coordinates are not valid.
     * @param latitude The latitude coordinate we want to validate and set
     * @throws IllegalArgumentException Gets thrown if coordinate is not valid.
     */
    public void setFrom_latitude(double latitude)
    {
        if (Address.isValidLatitude(latitude))
            this.from_latitude = latitude;
        else
            throw new IllegalArgumentException("Latitude coordinates must be > -90 and < 90!");
    }

    /**
     * Setter for the from_longitude member. Throws an exception if the longitude coordinates are not valid.
     * @param longitude The longitude coordinate we want to validate and set
     * @throws IllegalArgumentException Gets thrown if coordinate is not valid.
     */
    public void setFrom_longitude(double longitude)
    {
        if (Address.isValidLongitude(longitude))
            this.from_longitude = longitude;
        else
            throw new IllegalArgumentException("Longitude coordinates must be > -180 and < 180");
    }

    public void setPickup_time(Date pickup_time)
    {
        this.pickup_time = pickup_time;
    }

    public void setDropoff_time(Date dropoff_time)
    {
        this.dropoff_time = dropoff_time;
    }


    /**
     * Compares this instance of the class to another instance of this class to see if they are equal. This is made
     * for testing purposes.
     * @param otherJob The other DeliveryJob Object we need to compare to
     */
    public boolean equals(RideShareJob otherJob)
    {
        if (this.job_type.equals(otherJob.getJob_type()))
            if(this.customer_id == otherJob.getCustomer_id())
                if(this.school.equals(otherJob.getSchool()))
                    if(this.to_address.equals(otherJob.getTo_address()))
                        if(this.from_address.equals(otherJob.getFrom_address()))
                            if(this.num_passengers == otherJob.getNum_passengers())
                                if(this.to_latitude == otherJob.getTo_latitude())
                                    if(this.to_longitude == otherJob.getTo_longitude())
                                        if(this.from_latitude == otherJob.getFrom_latitude())
                                            if(this.from_longitude == otherJob.getFrom_longitude())
                                                if(this.pickup_time != null && this.pickup_time.equals(otherJob.getPickup_time()))
                                                    if(this.dropoff_time != null && this.dropoff_time.equals(otherJob.getDropoff_time()))
                                                        return true;

        return false;
    }

    /**
     * Converts the class instance state to a pretty printed JSON String, format should be
     * the same as what gets sent to a kafka topic.
     * @return A string of the current state of the RideShareJob Instance in pretty printed JSON format
     */
    @Override
    public String toString()
    {
        StringBuilder json = new StringBuilder();

        ObjectMapper mapper = new ObjectMapper();
        try
        {
            json.append(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this));
        }
        catch (IOException e)
        {
            // TODO: MAKE THIS LOG THE ERROR SINCE THIS SHOULDN'T BREAK ANYTHING (IT'S ONLY USED FOR PRINT STATEMENTS)
            e.printStackTrace();
        }

        return json.toString();
    }




}
