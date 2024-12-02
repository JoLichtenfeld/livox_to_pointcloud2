#ifndef LIVOX_TO_POINTCLOUD2_HPP
#define LIVOX_TO_POINTCLOUD2_HPP

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/point_cloud2.hpp"
#include "livox_ros_driver2/msg/custom_msg.hpp"
#include "std_msgs/msg/header.hpp"
#include "sensor_msgs/msg/point_field.hpp"

class LivoxToPointCloud2 : public rclcpp::Node
{
public:
    LivoxToPointCloud2();

private:
    void search_topics();
    void callback(const livox_ros_driver2::msg::CustomMsg::SharedPtr msg, size_t index);
    rclcpp::TimerBase::SharedPtr timer_;
    std::vector<rclcpp::Subscription<livox_ros_driver2::msg::CustomMsg>::SharedPtr> subscribers_;
    std::vector<rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr> publishers_;
    std::string topic_type_;
    size_t sub_count_;
};

#endif  // LIVOX_TO_POINTCLOUD2_HPP
