import launch
from launch import LaunchDescription
from launch_ros.actions import Node

def generate_launch_description():
    input_topic = launch.substitutions.LaunchConfiguration('input_topic', default='')
    output_topic = launch.substitutions.LaunchConfiguration('output_topic', default='')

    return LaunchDescription([
        launch.actions.DeclareLaunchArgument('input_topic', default_value='', 
                                              description='Input topic to subscribe to'),
        launch.actions.DeclareLaunchArgument('output_topic', default_value='', 
                                              description='Output topic to publish to'),

        Node(
            package='livox_to_pointcloud2',
            executable='livox_to_pointcloud2_node',
            name='livox_to_pointcloud2_node',
            output='screen',
            parameters=[{
                'input_topic': input_topic,
                'output_topic': output_topic,
            }]
        )
    ])