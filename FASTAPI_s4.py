from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from datetime import datetime

# Load the CSV file
try:
    df = pd.read_csv('device_data.csv', parse_dates=['Timestamp'])
    df.set_index('Timestamp', inplace=True)
    
    # Get the actual data range
    data_start = df.index.min().strftime('%m/%d/%Y %H:%M:%S')
    data_end = df.index.max().strftime('%m/%d/%Y %H:%M:%S')
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
    from_timestamp: str = Query(..., description="Start timestamp in format 'MM/DD/YYYY HH:MM:SS'"),
    to_timestamp: str = Query(..., description="End timestamp in format 'MM/DD/YYYY HH:MM:SS'")
):
    try:
        # Parse the input timestamps
        from_dt = datetime.strptime(from_timestamp, '%m/%d/%Y %H:%M:%S')
        to_dt = datetime.strptime(to_timestamp, '%m/%d/%Y %H:%M:%S')
        
        # Check if the requested range is within the available data range
        if from_dt < df.index.min() or to_dt > df.index.max():
            raise HTTPException(status_code=400, detail=f"Requested date range is outside the available data range ({data_start} to {data_end})")
        
        # Get the data for the specified timestamp range
        mask = (df.index >= from_dt) & (df.index <= to_dt)
        data_range = df.loc[mask]
        
        # Convert the data to a list of dictionaries (JSON-serializable format)
        data_list = []
        for timestamp, row in data_range.iterrows():
            data_list.append({
                "timestamp": timestamp.strftime('%m/%d/%Y %H:%M:%S'),
                "temperature": round(row['Temperature [°C]'], 2),
                "humidity": round(row['Humidity [%]'], 2),
                "wind_speed": round(row['Wind speed [m/s]'], 2),
                "wind_direction": round(row['Wind direction [°]'], 2),
                "air_pressure": round(row['Air pressure [hPa]'], 2),
                "consumption": round(row['Consumption (kWh)'], 2)
            })
        
        # Prepare the response
        response = {
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "data": data_list
        }
        
        return response
    
    except KeyError:
        raise HTTPException(status_code=404, detail="Data not found for the specified timestamp range")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid timestamp format. Please use 'MM/DD/YYYY HH:MM:SS'")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
