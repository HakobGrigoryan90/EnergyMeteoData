from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from datetime import datetime

# Load the CSV file
try:
    df = pd.read_csv('device_data.csv', parse_dates=['Date'])
    df.set_index('Date', inplace=True)
    
    # Get the actual data range
    data_start = df.index.min().strftime('%Y-%m-%d')
    data_end = df.index.max().strftime('%Y-%m-%d')
except FileNotFoundError:
    raise RuntimeError("The file 'device_data.csv' was not found. Please ensure the file is in the correct location.")

# Create FastAPI app instance
app = FastAPI(title="Device Data API",
              description=f"This API provides access to device data from {data_start} to {data_end}.")

@app.get("/api/data_info")
async def get_data_info():
    return {
        "data_range": {
            "start": data_start,
            "end": data_end
        },
        "total_records": len(df)
    }

@app.get("/api/get_data_range")
async def get_data_range(
    from_date: str = Query(..., description="Start date in format 'YYYY-MM-DD'"),
    to_date: str = Query(..., description="End date in format 'YYYY-MM-DD'")
):
    try:
        # Parse the input dates
        from_dt = datetime.strptime(from_date, '%Y-%m-%d').date()
        to_dt = datetime.strptime(to_date, '%Y-%m-%d').date()
        
        # Check if the requested range is within the available data range
        if from_dt < df.index.min().date() or to_dt > df.index.max().date():
            raise HTTPException(status_code=400, detail=f"Requested date range is outside the available data range ({data_start} to {data_end})")
        
        # Get the data for the specified date range
        mask = (df.index.date >= from_dt) & (df.index.date <= to_dt)
        data_range = df.loc[mask]
        
        # Convert the data to a list of dictionaries (JSON-serializable format)
        data_list = []
        for date, row in data_range.iterrows():
            data_list.append({
                "date": timestamp.strftime('%m/%d/%Y %H:%M:%S'),
                "average_temperature": round(row['Average temperature [°C]'], 2),
                "average_humidity": round(row['Average relative humidity [%]'], 2),
                "wind_speed": round(row['Wind speed [m/s]'], 2),
                "average_wind_direction": round(row['Average wind direction [°]'], 2),
                "precipitation": round(row['Precipitation [mm]'], 2),
                "average_air_pressure": round(row['Average air pressure [hPa]'], 2),
                "consumption": round(row['Consumption (kWh)'], 2)
            })
        
        # Prepare the response
        response = {
            "from_date": from_date,
            "to_date": to_date,
            "data": data_list
        }
        
        return response
    
    except KeyError:
        raise HTTPException(status_code=404, detail="Data not found for the specified date range")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use 'YYYY-MM-DD'")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
