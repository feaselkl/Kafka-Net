# Kafka-Net
This is the code used in my Kafka For .NET Developers presentation (http://www.catallaxyservices.com/presentations/kafka/).

To get the flight data we loaded and the airport data we used to enrich our data sets, check out the Data expo '09 contest's set of 
flight data (http://stat-computing.org/dataexpo/2009/the-data.html), including the 2008 flight data (http://stat-computing.org/dataexpo/2009/2008.csv.bz2)
and airports CSV file (http://stat-computing.org/dataexpo/2009/airports.csv).

All source code is licensed under the terms offered by the GPL (http://www.gnu.org/licenses/gpl.html).

## Visual Studio Version Warning

This solution was upgraded to run in Visual Studio 2017.  Unfortunately, there appears to be a change in VS2017 from VS2015, where F# projects will run in one or the other, but not both.  If you get an error
complaining about FSharp.Core 4.4.1.0, the easiest solution might be to install Visual Studio 2017 and run the solution from it.  Otherwise, you could modify the fsproj files,
changing TargetFSharpCoreVersion from 4.4.1.0 to 4.4.0.0.  That might do the trick, though I have not confirmed this.