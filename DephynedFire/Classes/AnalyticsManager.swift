//
//  AnalyticsManager.swift
//  Graffiti
//
//  Created by Adebayo Ijidakinro on 10/20/19.
//  Copyright © 2019 Dephyned. All rights reserved.
//

import Foundation
import FirebaseAnalytics
import FirebaseAuth

open class AnalyticsManager {
    
    static let kUserId = "user_id"
    
    public enum ContentType: String {
        case Spot
        case SharedSpot
        case User
    }
    
    public enum Actions: String {
        case Shared
        case Liked
        case Disliked
        case HashtagClicked
        case SearchClicked
        case MainMenuSearchClicked
        case AddSpotClicked
        case ImportSpotsClicked
        case MenuClicked
        case MapClicked
        case ShareClicked
        case SpotMenuClicked
        case SpotReported
        case UserReported
        case NotificationsClicked
        case UsersAndHashtagsClicked
        case SettingsClicked
        case ProfileButtonClicked
        case SharedWithYouButtonClicked
        case MainMenuInviteFriendsClicked
        case HashtagsFollowingClicked
        case HashtagsUpdated
        case SettingsInviteFriendsClicked
        case CommentClicked
        case ProfileScreenImageClicked
        case SpotProfileButtonClicked
        
        case HashtagFollowed
        case HashtagUnfollowed
        
        case Followed
        case Unfollowed
        
        case FollowersRetrieved
        case FollowingRetrieved
        
        case UserSearchedOnProfileScreen
        case UsersSearched
        case SpotCreated
        case SharedSpotsDownloaded
        case SpotDeleted
        case SpotUpdated
        case HashtagsSaved
        case SpotsDownloaded
        case SpotsSavedToCache
        case SpotsRefreshed
        case UserBlocked
        case SpotsDownloadedForCurrentUser
        case UsernameUpdated
        case HashtagsDownloaded
        case AllActivitiesDownloaded
        case ActivitySent
        case SpotsRetrievedFromCache
        case SpotAddedToWall
        case MapImageButtonClicked
        case SpotSavedToPurchasedList
        
        case PhotoAccessAllowed
        case PhotoAccessDenied
        
        case AllowNotifications
        case NotificationsNotAllowed
        
        case AllowLocation
        case LocationNotAllowed
        
        case SelectAddressClicked
        case CustomAddressSelected
        case NearbyLocationsScreenShown
        case AdClicked
        
        case WallCreated
    }
    
    public static func logMethodEvent(name: String) {
        
        guard
            let userId = Auth.auth().currentUser?.uid
        else { return }
        
        Analytics.logEvent(name, parameters: [            
            kUserId: userId
        ])
    }
    
    public static func logSearch (searchTerm:String) {
        
        guard
            let userId = Auth.auth().currentUser?.uid
        else { return }
        
        Analytics.logEvent(Actions.UsersSearched.rawValue, parameters: [
            AnalyticsParameterSearchTerm: searchTerm,
            kUserId: userId
        ])
    }
    
    public static func logContentAction (type: ContentType, action: Actions, id: String?) {
        
        guard
            let id = id,
            let userId = Auth.auth().currentUser?.uid
        else { return }
        
        Analytics.logEvent(action.rawValue, parameters: [
            AnalyticsEventSelectContent: action.rawValue,
            AnalyticsParameterContentType: type.rawValue,
            AnalyticsParameterItemID: id,
            kUserId: userId
        ])
    }
    
    public static func logGenericEvent (name: Actions) {
        
        guard
            let userId = Auth.auth().currentUser?.uid
        else { return }
        
        Analytics.logEvent(name.rawValue, parameters: [
            kUserId: userId
        ])
        
    }
    
    public static func logError (message:String) {
        Analytics.logEvent(message, parameters: nil)
    }
}
