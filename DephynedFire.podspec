#
# Be sure to run `pod lib lint DephynedFire.podspec' to ensure this is a
# valid spec before submitting.
#
# Any lines starting with a # are optional, but their use is encouraged
# To learn more about a Podspec see https://guides.cocoapods.org/syntax/podspec.html
#

Pod::Spec.new do |s|
  s.name             = 'DephynedFire'
  s.version          = '1.0.31'
  s.summary          = 'An easy way to handle Firebase, Firestore, and AWS services.'

# This description is used to generate tags and improve search results.
#   * Think: What does it do? Why did you write it? What is the focus?
#   * Try to keep it short, snappy and to the point.
#   * Write the description between the DESC delimiters below.
#   * Finally, don't worry about the indent, CocoaPods strips it!

  s.description      = <<-DESC
The purpose of this library is to make Firebase, Firestore, and AWS Service handling extremely simple
                       DESC

  s.homepage         = 'https://github.com/adeiji/DephynedFire'
  # s.screenshots     = 'www.example.com/screenshots_1', 'www.example.com/screenshots_2'
  s.license          = { :type => 'MIT', :file => 'LICENSE' }
  s.author           = { 'adeiji' => 'adeiji@yahoo.com' }
  s.source           = { :git => 'https://github.com/adeiji/DephynedFire.git', :tag => s.version.to_s }
  # s.social_media_url = 'https://twitter.com/<TWITTER_USERNAME>'

  s.ios.deployment_target = '13.0'

  s.source_files = 'DephynedFire/Classes/**/*'
  
  # s.resource_bundles = {
  #   'DephynedFire' => ['DephynedFire/Assets/*.png']
  # }

  # s.public_header_files = 'Pod/Classes/**/*.h'
  # s.frameworks = 'UIKit', 'MapKit'
  # s.dependency 'AFNetworking', '~> 2.3'
  
  # Allow static frameworks
  s.static_framework = true
  
  s.dependency 'SnapKit'
  s.dependency 'Firebase/Auth'
  s.dependency 'SwiftLocation'
  s.dependency 'Firebase/Core'
  s.dependency 'Firebase/Firestore'
  s.dependency 'Firebase/Storage'
  s.dependency 'Firebase/Functions'
  s.dependency 'RxSwift'
  s.dependency 'RxCocoa'
  s.dependency 'AWSCore'
  s.dependency 'AWSS3'
  s.dependency 'AWSS3', '~> 2.11.0'
  s.dependency 'AWSMobileClient', '~> 2.11.0'
  s.dependency 'GeoFire', '~> 4.0'
end
