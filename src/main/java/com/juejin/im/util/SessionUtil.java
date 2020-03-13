package com.juejin.im.util;

import com.juejin.im.attribute.Attributes;
import com.juejin.im.session.Session;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;

import java.util.HashMap;
import java.util.Map;

public class SessionUtil {

    private static Map<String, Channel> userIdToChannels = new HashMap<>();

    private static Map<String, ChannelGroup> groupIdToChannelGroup = new HashMap<>();

    public static void bindSession(Session session, Channel channel){
        userIdToChannels.put(session.getUserId(), channel);
        channel.attr(Attributes.SESSION).set(session);
    }

    public static void unBindSession(Channel channel){
        if (hasLogin(channel)){
            userIdToChannels.remove(getSession(channel).getUserId());
            channel.attr(Attributes.SESSION).set(null);
        }
    }

    public static boolean hasLogin(Channel channel) {
        return channel.hasAttr(Attributes.SESSION);
    }

    public static Session getSession(Channel channel){
        return channel.attr(Attributes.SESSION).get();
    }

    public static Channel getChannel(String userId){
        return userIdToChannels.get(userId);
    }

    public static void bindChannelGroup(String groupId, ChannelGroup channelGroup){
        groupIdToChannelGroup.put(groupId, channelGroup);
    }

    public static ChannelGroup getChannelGroup(String groupId){
        return groupIdToChannelGroup.get(groupId);
    }

}
