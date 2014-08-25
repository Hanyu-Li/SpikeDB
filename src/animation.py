from tempfile import NamedTemporaryFile
VIDEO_TAG = """<video controls>
 <source src="data:video/x-m4v;base64,{0}" type="video/mp4">
 Your browser does not support the video tag.
</video>"""

def anim_to_html(anim):
    if not hasattr(anim, '_encoded_video'):
        with NamedTemporaryFile(suffix='.mp4') as f:
            anim.save(f.name, fps=20, extra_args=['-vcodec', 'libx264','-pix_fmt', 'yuv420p'])
            video = open(f.name, "rb").read()
        anim._encoded_video = video.encode("base64")
    
    return VIDEO_TAG.format(anim._encoded_video)

from IPython.display import HTML

def display_animation(anim):
    plt.close(anim._fig)
    return HTML(anim_to_html(anim))

"""
animated plot In 3D
"""
import numpy as np
import matplotlib.pyplot as plt
import mpl_toolkits.mplot3d.axes3d as p3
import matplotlib.animation as animation

def update_lines(num, dataLines, lines) :
    for line, data in zip(lines, dataLines) :
        # NOTE: there is no .set_data() for 3 dim data...
        #print data
        line.set_data(data[0:2, :num])
        line.set_3d_properties(data[2,:num])
    return lines

# Attaching 3D axis to the figure
def plot_3d_trace(input):
    fig = plt.figure()
    #ax = p3.Axes3D(fig)
    ax = fig.add_subplot(111,projection = '3d')
    
    data = [input]
    #print data
    
    # NOTE: Can't pass empty arrays into 3d version of plot()
    lines = [ax.plot(dat[0, 0:1], dat[1, 0:1], dat[2, 0:1])[0] for dat in data]
    
    # Setting the axes properties
    ax.set_xlim3d([5, 50])
    ax.set_xlabel('Concentration, [ppm]')
    
    ax.set_ylim3d([-1000, 1000])
    ax.set_ylabel('Gradient, [ppm/s]')
    
    ax.set_zlim3d([0.0, 450])
    ax.set_zlabel('Frequency, [Hz]')
    
    ax.set_title('3D Trace Video')
    
    # Creating the Animation object
    line_ani = animation.FuncAnimation(fig, update_lines, 100, fargs=(data, lines),
                                  interval=50, blit=False)
    
    #return line_ani
    return display_animation(line_ani)
    #plt.show()

    
