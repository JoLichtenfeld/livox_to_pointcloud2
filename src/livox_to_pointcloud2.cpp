#include "livox_to_pointcloud2.hpp"

using namespace std::chrono_literals;

LivoxToPointCloud2::LivoxToPointCloud2() : Node("livox_to_pointcloud2")
{
    sub_count_ = 0;
    topic_type_ = "livox_ros_driver2/msg/CustomMsg";

    std::string input_topic;
    std::string output_topic;
    this->declare_parameter("input_topic", "");
    this->declare_parameter("output_topic", "");
    this->get_parameter("input_topic", input_topic);
    this->get_parameter("output_topic", output_topic);

    if (!input_topic.empty())
    {
        auto subscriber = this->create_subscription<livox_ros_driver2::msg::CustomMsg>(
            input_topic, 10, [this](const livox_ros_driver2::msg::CustomMsg::SharedPtr msg) {
                this->callback(msg, 0);
            });
        if (output_topic.empty())
        {
            output_topic = input_topic + "_pc2";
        }
        auto publisher = this->create_publisher<sensor_msgs::msg::PointCloud2>(output_topic, 10);
        RCLCPP_INFO(this->get_logger(), "Subscribed to %s", input_topic.c_str());
        RCLCPP_INFO(this->get_logger(), "Publishing to %s", output_topic.c_str());
        subscribers_.push_back(subscriber);
        publishers_.push_back(publisher);
        sub_count_++;
    }
    else
    {
        timer_ = this->create_timer(std::chrono::milliseconds(500), std::bind(&LivoxToPointCloud2::search_topics, this));
    }
    RCLCPP_INFO(this->get_logger(), "LivoxToPointCloud2 node started");
}

void LivoxToPointCloud2::search_topics()
{
    RCLCPP_INFO_ONCE(this->get_logger(), "Searching for topics of type %s", topic_type_.c_str());
    auto topics_and_types = this->get_topic_names_and_types();
    for (const auto& topic_and_type : topics_and_types)
    {
        const std::string topic_name = topic_and_type.first;
        const std::vector<std::string> types = topic_and_type.second;

        if (std::count_if(subscribers_.begin(), subscribers_.end(), [&topic_name](const auto& subscriber) {
                return subscriber->get_topic_name() == topic_name;
            }) > 0)
        {
            continue;
        }

        if (std::find(types.begin(), types.end(), topic_type_) != types.end())
        {
            auto sub_index = sub_count_;
            auto subscriber = this->create_subscription<livox_ros_driver2::msg::CustomMsg>(
                topic_name, 10, [this, sub_index](const livox_ros_driver2::msg::CustomMsg::SharedPtr msg) {
                    this->callback(msg, sub_index);
                });
            auto publisher = this->create_publisher<sensor_msgs::msg::PointCloud2>(topic_name + "_pc2", 10);
            RCLCPP_INFO(this->get_logger(), "Subscribed to %s", topic_name.c_str());
                        
            subscribers_.push_back(subscriber);
            publishers_.push_back(publisher);
            sub_count_++;
        }
    }

    if (subscribers_.empty())
    {
        RCLCPP_WARN_ONCE(this->get_logger(), "No topics of type %s found", topic_type_.c_str());
        return;
    }
}

void LivoxToPointCloud2::callback(const livox_ros_driver2::msg::CustomMsg::SharedPtr msg, size_t index)
{
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

    publishers_[index]->publish(output);
}
