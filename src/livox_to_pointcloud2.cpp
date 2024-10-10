#include "livox_to_pointcloud2.hpp"

using namespace std::chrono_literals;

LivoxToPointCloud2::LivoxToPointCloud2() : Node("livox_to_pointcloud2")
{
    std::string custom_type = "livox_ros_driver2/msg/CustomMsg";
    std::string sub_topic;
    std::string pub_topic;

    while (rclcpp::ok() && sub_topic.empty())
    {
        auto topics_and_types = this->get_topic_names_and_types();
        for (const auto& topic_and_type : topics_and_types)
        {
            const std::string topic_name = topic_and_type.first;
            const std::vector<std::string> types = topic_and_type.second;
            if (std::find(types.begin(), types.end(), custom_type) != types.end())
            {
                sub_topic = topic_name;
                pub_topic = topic_name + "_pc2";
                RCLCPP_INFO(this->get_logger(), "Subscribing to topic: %s", topic_name.c_str());
                RCLCPP_INFO(this->get_logger(), "Publishing to topic: %s", pub_topic.c_str());
                break;
            }
        }
        RCLCPP_INFO(this->get_logger(), "No topic found, sleeping 1 sec");
        rclcpp::sleep_for(1000ms);
    }

    publisher_ = this->create_publisher<sensor_msgs::msg::PointCloud2>(pub_topic, 10);
    subscription_ = this->create_subscription<livox_ros_driver2::msg::CustomMsg>(
        sub_topic, 10, std::bind(&LivoxToPointCloud2::callback, this, std::placeholders::_1));
    RCLCPP_INFO(this->get_logger(), "LivoxToPointCloud2 node has been started");
}

void LivoxToPointCloud2::callback(const livox_ros_driver2::msg::CustomMsg::SharedPtr msg)
{
    RCLCPP_INFO_ONCE(this->get_logger(), "First msg received");
    sensor_msgs::msg::PointCloud2 output;
    output.header = msg->header;
    output.fields.resize(6);

    output.fields[0].name = "x";
    output.fields[0].offset = 0;
    output.fields[0].datatype = sensor_msgs::msg::PointField::FLOAT32;
    output.fields[0].count = 1;

    output.fields[1].name = "y";
    output.fields[1].offset = 4;
    output.fields[1].datatype = sensor_msgs::msg::PointField::FLOAT32;
    output.fields[1].count = 1;

    output.fields[2].name = "z";
    output.fields[2].offset = 8;
    output.fields[2].datatype = sensor_msgs::msg::PointField::FLOAT32;
    output.fields[2].count = 1;

    output.fields[3].name = "intensity";
    output.fields[3].offset = 12;
    output.fields[3].datatype = sensor_msgs::msg::PointField::FLOAT32;
    output.fields[3].count = 1;

    output.fields[4].name = "tag";
    output.fields[4].offset = 16;
    output.fields[4].datatype = sensor_msgs::msg::PointField::UINT8;
    output.fields[4].count = 1;

    output.fields[5].name = "line";
    output.fields[5].offset = 17;
    output.fields[5].datatype = sensor_msgs::msg::PointField::UINT8;
    output.fields[5].count = 1;

    output.point_step = 18;
    output.row_step = output.point_step * msg->point_num;
    output.data.resize(output.row_step);

    uint8_t* raw_data_ptr = output.data.data();
    for (const auto& point : msg->points)
    {
        *(reinterpret_cast<float*>(raw_data_ptr + 0)) = point.x;
        *(reinterpret_cast<float*>(raw_data_ptr + 4)) = point.y;
        *(reinterpret_cast<float*>(raw_data_ptr + 8)) = point.z;
        *(reinterpret_cast<float*>(raw_data_ptr + 12)) = static_cast<float>(point.reflectivity);
        *(raw_data_ptr + 16) = point.tag;
        *(raw_data_ptr + 17) = point.line;

        raw_data_ptr += output.point_step;
    }

    output.width = msg->point_num;
    output.height = 1;
    output.is_bigendian = false;
    output.is_dense = true;

    publisher_->publish(output);
}
