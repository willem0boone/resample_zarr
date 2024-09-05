import matplotlib.pyplot as plt
import xarray as xr
# import numpy as np
# import cartopy.crs as ccrs
# import cartopy.feature as cfeature
# from cartopy.mpl.gridliner import LatitudeFormatter
# from cartopy.mpl.gridliner import LongitudeFormatter
#
#
# def plot_nice(ds, var, name):
#     # Create a plot
#     fig, ax = plt.subplots(figsize=(10, 10), subplot_kw={'projection': ccrs.PlateCarree()})
#
#     # Add features to the plot
#     ax.add_feature(cfeature.LAND, edgecolor='black')
#     ax.add_feature(cfeature.OCEAN)
#     ax.add_feature(cfeature.COASTLINE)
#     ax.add_feature(cfeature.BORDERS, linestyle=':')
#
#     # Get values and coordinates
#     values = ds[var].values.squeeze()  # Remove dimensions of size 1
#     longitude = ds.longitude.values
#     latitude = ds.latitude.values
#
#     # Ensure the values array has the correct shape
#     # Create a meshgrid of the coordinates
#     lon, lat = np.meshgrid(longitude, latitude)
#
#     # Ensure dimensions match
#     if values.shape != lon.shape:
#         lon, lat = np.meshgrid(longitude, latitude)
#         if lon.shape != values.shape:
#             lon = lon[:values.shape[0], :values.shape[1]]
#             lat = lat[:values.shape[0], :values.shape[1]]
#
#     # Plot the data
#     im = ax.pcolormesh(lon, lat, values, cmap='viridis', shading='auto')
#
#     # Add colorbar
#     cbar = plt.colorbar(im, ax=ax, orientation='vertical', pad=0.02)
#     cbar.set_label('values')
#
#     # Set latitude and longitude ticks
#     ax.set_xticks(np.arange(np.floor(longitude.min()), np.ceil(longitude.max()), 10), crs=ccrs.PlateCarree())
#     ax.set_yticks(np.arange(np.floor(latitude.min()), np.ceil(latitude.max()), 10), crs=ccrs.PlateCarree())
#
#     # Format tick labels
#     lon_formatter = LongitudeFormatter(zero_direction_label=True)
#     lat_formatter = LatitudeFormatter()
#     ax.xaxis.set_major_formatter(lon_formatter)
#     ax.yaxis.set_major_formatter(lat_formatter)
#
#     # Add labels and title
#     ax.set_xlabel('Longitude')
#     ax.set_ylabel('Latitude')
#     ax.set_title(name)
#
#     plt.savefig(f"{name}.png")
#     plt.close()


def plot_dataset(
        ds: xr.Dataset,
        var: str,
        name: str
) -> None:
    """
    Plots a variable from an xarray Dataset as an image.

    This function extracts the specified variable from the xarray Dataset,
    visualizes it using `imshow`, and saves the plot as a PNG file.

    :param ds: The xarray Dataset containing the variable to plot.
    :type ds: xr.Dataset

    :param var: The name of the variable to plot from the Dataset.
    :type var: str

    :param name: The name to use for the output PNG file (excluding extension).
    :type name: str

    :return: None
    :rtype: None

    :raises KeyError: If the variable `var` is not found in the Dataset.
    :raises ValueError: If the Dataset does not contain the variable data.
    """
    # Check if the variable exists in the Dataset
    if var not in ds:
        raise KeyError(f"Variable '{var}' not found in the Dataset.")

    # Extract values and coordinates
    values = ds[var].values

    # Plot using imshow
    fig, ax = plt.subplots()
    cax = ax.imshow(values, cmap='viridis')

    # Add colorbar
    cbar = plt.colorbar(cax, ax=ax, orientation='vertical')
    cbar.set_label('Temperature')

    # Add labels and title
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(name)

    # Save the plot as a PNG file
    plt.savefig(f"{name}.png")
    plt.close()
