#include <rclcpp/rclcpp.hpp>
#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/point_field.hpp>
#include <livox_ros_driver2/msg/custom_msg.hpp>
#include <rosbag2_cpp/reader.hpp>
#include <rosbag2_cpp/writers/sequential_writer.hpp>
#include <rosbag2_storage/storage_options.hpp>
#include <rosbag2_cpp/converter_options.hpp>

#include <iostream>
#include <fstream>
#include <vector>

// using namespace rosbag2_cpp;
// using namespace rosbag2_storage;
using sensor_msgs::msg::PointCloud2;
using sensor_msgs::msg::PointField;
using livox_ros_driver2::msg::CustomMsg;

std::string add_suffix_before_last_slash(const std::string& path) {
    size_t lastSlashPos = path.find_last_of('/');
    if (lastSlashPos == std::string::npos) {
        return "Error: No slash found in the path.";
    }
    std::string newPath = path.substr(0, lastSlashPos) + "_pc2" + path.substr(lastSlashPos);
    return newPath;
}

void convert_to_pointcloud2(const CustomMsg &livox_msg, PointCloud2 &cloud) {
    cloud.header = livox_msg.header;

    // Define the fields (x, y, z, intensity, tag, line)
    cloud.fields.resize(6);
    cloud.fields[0].name = "x";
    cloud.fields[0].offset = 0;
    cloud.fields[0].datatype = PointField::FLOAT32;
    cloud.fields[0].count = 1;

    cloud.fields[1].name = "y";
    cloud.fields[1].offset = 4;
    cloud.fields[1].datatype = PointField::FLOAT32;
    cloud.fields[1].count = 1;

    cloud.fields[2].name = "z";
    cloud.fields[2].offset = 8;
    cloud.fields[2].datatype = PointField::FLOAT32;
    cloud.fields[2].count = 1;

    cloud.fields[3].name = "intensity";
    cloud.fields[3].offset = 12;
    cloud.fields[3].datatype = PointField::FLOAT32;
    cloud.fields[3].count = 1;

    cloud.fields[4].name = "tag";
    cloud.fields[4].offset = 16;
    cloud.fields[4].datatype = PointField::UINT8;
    cloud.fields[4].count = 1;

    cloud.fields[5].name = "line";
    cloud.fields[5].offset = 17;
    cloud.fields[5].datatype = PointField::UINT8;
    cloud.fields[5].count = 1;

    cloud.point_step = 18;
    cloud.row_step = cloud.point_step * livox_msg.point_num;
    cloud.data.resize(cloud.row_step);

    uint8_t *raw_data_ptr = cloud.data.data();
    for (const auto &point : livox_msg.points) {
        *reinterpret_cast<float *>(raw_data_ptr + 0) = point.x;
        *reinterpret_cast<float *>(raw_data_ptr + 4) = point.y;
        *reinterpret_cast<float *>(raw_data_ptr + 8) = point.z;
        *reinterpret_cast<float *>(raw_data_ptr + 12) = static_cast<float>(point.reflectivity);
        raw_data_ptr[16] = point.tag;
        raw_data_ptr[17] = point.line;
        raw_data_ptr += cloud.point_step;
    }

    cloud.width = livox_msg.point_num;
    cloud.height = 1;
    cloud.is_bigendian = false;
    cloud.is_dense = true;
}

void process_bag(const std::string &bag_file) {
    // Initialize bag reader with storage options (MCAP support)
    rosbag2_storage::StorageOptions storage_options;
    storage_options.uri = bag_file;
    storage_options.storage_id = "mcap";  // Specify MCAP as storage format

    rosbag2_cpp::ConverterOptions converter_options;
    converter_options.input_serialization_format = "cdr";
    converter_options.output_serialization_format = "cdr";

    rosbag2_cpp::Reader reader;
    reader.open(storage_options, converter_options);
    rosbag2_storage::StorageOptions new_storage_options = storage_options;
    new_storage_options.uri = add_suffix_before_last_slash(storage_options.uri);
    rosbag2_cpp::writers::SequentialWriter writer;
    writer.open(new_storage_options, converter_options);

    // Get all topics and filter for CustomMsg
    const auto topics = reader.get_all_topics_and_types();
    std::vector<std::string> livox_topics;
    for (const auto &topic : topics) {
        if (topic.type == "livox_ros_driver2/msg/CustomMsg") {
            livox_topics.push_back(topic.name);
            rosbag2_storage::TopicMetadata pc2_topic = topic;
            pc2_topic.type = "sensor_msgs/msg/PointCloud2";
            pc2_topic.name = topic.name + "_pc2";
            writer.create_topic(pc2_topic);
        }
        writer.create_topic(topic);
    }

    if (livox_topics.empty()) {
        std::cerr << "No Livox CustomMsg topics found in the bag file: " << bag_file << std::endl;
        return;
    }

    std::cout << "Found Livox topics: ";
    for (const auto &topic : livox_topics) {
        std::cout << topic << " ";
    }
    std::cout << std::endl;

    // Read messages from bag and convert them
    while (reader.has_next()) {
        rosbag2_storage::SerializedBagMessageSharedPtr msg = reader.read_next();
        if (std::find(livox_topics.begin(), livox_topics.end(), msg->topic_name) != livox_topics.end())
        {
            // deserialize
            rclcpp::Serialization<CustomMsg> custom_serialization;
            rclcpp::SerializedMessage custom_serialized_msg(*msg->serialized_data);
            CustomMsg::SharedPtr custom_msg = std::make_shared<CustomMsg>();
            custom_serialization.deserialize_message(&custom_serialized_msg, custom_msg.get());

            // convert
            PointCloud2::SharedPtr pointcloud2_msg = std::make_shared<PointCloud2>();
            convert_to_pointcloud2(*custom_msg, *pointcloud2_msg);

            // serialize
            rclcpp::Serialization<CustomMsg> pc2_serialization;
            rclcpp::SerializedMessage pc2_serialized_message;
            pc2_serialization.serialize_message(pointcloud2_msg.get(), &pc2_serialized_message);

            // prepare for bag
            rosbag2_storage::SerializedBagMessageSharedPtr new_msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
            new_msg->topic_name = msg->topic_name + "_pc2";
            new_msg->recv_timestamp = msg->recv_timestamp;
            new_msg->send_timestamp = msg->send_timestamp;
            new_msg->serialized_data = msg->serialized_data;

            writer.write(new_msg);
        }

        writer.write(msg);

    }
    writer.close();
    std::cout << "Closing bag file " << bag_file << std::endl;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: livox_bag_converter <path_to_bag_file>" << std::endl;
        return 1;
    }

    std::string bag_file = argv[1];

    if (!rcpputils::fs::exists(bag_file)) {
        std::cerr << "Bag file does not exist: " << bag_file << std::endl;
        return 1;
    }

    process_bag(bag_file);
    return 0;
}
