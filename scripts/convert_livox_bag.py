#!/usr/bin/env python3

import rosbag2_py
from sensor_msgs.msg import PointCloud2, PointField
from livox_ros_driver2.msg import CustomMsg
import struct
import sys
import os

def convert_to_pointcloud2(livox_msg):
    pointcloud2_msg = PointCloud2()
    pointcloud2_msg.header = livox_msg.header
    pointcloud2_msg.fields = [
        PointField(name='x', offset=0, datatype=PointField.FLOAT32, count=1),
        PointField(name='y', offset=4, datatype=PointField.FLOAT32, count=1),
        PointField(name='z', offset=8, datatype=PointField.FLOAT32, count=1),
        PointField(name='intensity', offset=12, datatype=PointField.FLOAT32, count=1),
        PointField(name='tag', offset=16, datatype=PointField.UINT8, count=1),
        PointField(name='line', offset=17, datatype=PointField.UINT8, count=1)
    ]
    pointcloud2_msg.point_step = 18
    pointcloud2_msg.row_step = pointcloud2_msg.point_step * livox_msg.point_num
    pointcloud2_msg.data = bytearray(pointcloud2_msg.row_step)

    raw_data_ptr = 0
    for point in livox_msg.points:
        struct.pack_into('fff', pointcloud2_msg.data, raw_data_ptr, point.x, point.y, point.z)
        struct.pack_into('f', pointcloud2_msg.data, raw_data_ptr + 12, float(point.reflectivity))
        pointcloud2_msg.data[raw_data_ptr + 16] = point.tag
        pointcloud2_msg.data[raw_data_ptr + 17] = point.line
        raw_data_ptr += pointcloud2_msg.point_step

    pointcloud2_msg.width = livox_msg.point_num
    pointcloud2_msg.height = 1
    pointcloud2_msg.is_bigendian = False
    pointcloud2_msg.is_dense = True

    return pointcloud2_msg

def process_bag(bag_file):
    # Initialize bag reader
    storage_options = rosbag2_py.StorageOptions(uri=bag_file, storage_id='mcap')
    converter_options = rosbag2_py.ConverterOptions(input_serialization_format='cdr', output_serialization_format='cdr')
    reader = rosbag2_py.SequentialReader()
    reader.open(storage_options, converter_options)

    # Get all available topics and types
    topics = reader.get_all_topics_and_types()

    # Filter for topics of type CustomMsg (Livox point cloud messages)
    livox_topics = [topic.name for topic in topics if topic.type == 'livox_ros_driver2/msg/CustomMsg']

    if not livox_topics:
        print(f"No Livox CustomMsg topics found in the bag file {bag_file}")
        return

    print(f"Found Livox topics: {livox_topics}")

    while reader.has_next():
        topic, data, timestamp = reader.read_next()

        if topic in livox_topics:
            # Deserialize CustomMsg
            msg = CustomMsg().deserialize(data)

            # Convert to PointCloud2
            pointcloud2_msg = convert_to_pointcloud2(msg)

            # Process or save the converted pointcloud2_msg here
            # For now, just print a confirmation message
            print(f"Converted message from topic {topic} at time {timestamp}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python livox_bag_converter.py <path_to_bag_file>")
        sys.exit(1)

    bag_file = sys.argv[1]

    if not os.path.exists(bag_file):
        print(f"Bag file {bag_file} does not exist.")
        sys.exit(1)

    process_bag(bag_file)

if __name__ == '__main__':
    main()
