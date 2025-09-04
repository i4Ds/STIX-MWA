
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from helper_functions.utils import get_root_path_to_data
from helper_functions.mwa import plot_mwa_from_flare_row, plot_mwa_from_obs_ids, plot_mwa_light_curve
from helper_functions.stix import plot_stix_light_curve, get_position
from helper_functions.ecallisto import plot_ecallistio
import logging


def plot_flare(save_path, row=None, obs_ids=None):
    """
    plots spectrograms using either manually specified observation IDs or flare metadata
    """
    fig, axes, cbar_gs = create_figure_and_axes(subplots=5)
    time_axis = []
    if row is not None:
        try:
            plot_stix_light_curve(row, axes[0], energy_range=(0, 4))
        except Exception as e:
            logging.info(f"Error plotting STIX light curve: {e}")
            axes[0].text(0.5, 0.5, 'STIX data not available', ha='center', va='center')
        try:
            spec, time_axis = plot_mwa_from_flare_row(row, axes[1], fig, cbar_gs, get_root_path_to_data())
        except Exception as e:
            logging.info(f"Error plotting MWA data: {e}")
            axes[1].text(0.5, 0.5, 'MWA spectrogram not available', ha='center', va='center')
    elif obs_ids is not None:
        try:
            spec, time_axis = plot_mwa_from_obs_ids(obs_ids, axes, cbar_gs, fig, get_root_path_to_data())
        except Exception as e:
            logging.info(f"Error plotting MWA data: {e}")
            axes[1].text(0.5, 0.5, 'MWA spectrogram not available', ha='center', va='center')
    else:
        raise ValueError("Either row or obs_ids must be provided.")

    if not time_axis:
        return True

    try:
        plot_mwa_light_curve(spec, time_axis, axes[2], row)
    except Exception as e:
        logging.info(f"Error plotting MWA light curve: {e}")
        axes[2].text(0.5, 0.5, 'MWA light curve not available', ha='center', va='center')
    if row is not None:
        try:
            plot_positions(time_axis, axes[3])
        except Exception as e:
            logging.info(f"Error plotting positions: {e}")
            axes[3].text(0.5, 0.5, 'Positions not available', ha='center', va='center')
        try:
            plot_ecallistio(row, axes[4], fig, cbar_gs)
        except Exception as e:
            logging.info(f"Error plotting e-Callisto data: {e}")
            axes[4].text(0.5, 0.5, 'e-Callisto data not available', ha='center', va='center')
    finalize_plot(fig, save_path)
    plt.close(fig)

    return False


def create_figure_and_axes(subplots=5):
    fig = plt.figure(figsize=(10, 18))
    gs = GridSpec(subplots, 2, width_ratios=[1, 0.05], height_ratios=[1 for _ in range(subplots)])
    axes = [fig.add_subplot(gs[i, 0]) for i in range(subplots)]
    return fig, axes, gs


def finalize_plot(fig, save_path):
    # save the plot to a file and close the figure
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close(fig)


def plot_positions(time_axis, ax):
    """ 
    plots the positions of the Sun, Earth, and SOLO spacecraft at the start and end of the time axis 
    """
    if len(time_axis) == 0 or time_axis is None:
        ax.text(0.5, 0.5, 'MWA spectrogram not available!', ha='center', va='center')
        return ax
    
    start = time_axis[0]
    end = time_axis[-1]

    emph = get_position(start, end)
    orbit = emph.data['orbit']

     # get solo position (first entry is enough since steps=1)
    x_solo = orbit['x'][0]
    y_solo = orbit['y'][0]

     # get earth position
    earth_x = orbit['objects']['earth']['x'][0]
    earth_y = orbit['objects']['earth']['y'][0]

    plot_object(ax, 0, 0, "Sun", "yellow", 1000)
    plot_object(ax, earth_x, earth_y, "Earth", "green", 250)
    plot_object(ax, x_solo, y_solo, "SOLO", "orange", 50)

     # formatting
    ax.set_title(f"SOLO Location at {orbit['utc'][0]}")
    ax.set_xlabel('X (au)')
    ax.set_ylabel('Y (au)')
    ax.set_xlim(-2, 2)
    ax.set_ylim(-2, 2)
    ax.set_aspect('equal')
    ax.grid(True)


def plot_object(ax, x, y, label, color, size, marker='o', zorder=3):
    """ 
    plots a single object on the given axis 
    """
    ax.scatter(x, y, s=size, c=color, label=label, edgecolors='black', marker=marker, zorder=zorder)
    ax.text(x, y, label, ha='center', va='center', zorder=zorder+1)
