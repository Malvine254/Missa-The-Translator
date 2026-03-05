import { OpenAIChatModel } from '@microsoft/teams.openai';
import { ChatPrompt } from '@microsoft/teams.ai';
import config from './config';
import { ChatMessage } from './graphApiHelper';

class SummarizationHelper {
  private model: OpenAIChatModel;

  constructor() {
    this.model = new OpenAIChatModel({
      model: config.azureOpenAIDeploymentName,
      apiKey: config.azureOpenAIKey,
      endpoint: config.azureOpenAIEndpoint,
      apiVersion: '2024-10-21',
    });
  }

  /**
   * Summarize chat messages into a concise overview
   */
  async summarizeChatMessages(
    messages: ChatMessage[],
    summaryLength: 'brief' | 'detailed' = 'detailed'
  ): Promise<string> {
    try {
      console.log(`[SUMMARIZE] Starting summarization (${summaryLength} mode, ${messages.length} messages)`);
      
      if (messages.length === 0) {
        console.log(`[SUMMARIZE] No messages to summarize, returning empty message`);
        return 'No messages to summarize.';
      }

      // Format messages for summarization - filter out system/event messages with null from or empty content
      const formattedMessages = messages
        .reverse()
        .filter((msg) => msg.from != null && msg.body?.content && msg.body.content.trim().length > 0 && msg.messageType !== 'systemEventMessage')
        .map((msg) => `${msg.from?.user?.displayName || 'Unknown'}: ${msg.body.content.replace(/<[^>]*>/g, '').trim()}`)
        .filter((line) => line.length > 10)
        .join('\n');

      if (!formattedMessages) {
        return 'No user messages found to summarize (all messages were system events).';
      }

      const lengthInstruction =
        summaryLength === 'brief'
          ? 'Provide a brief 2-3 sentence summary.'
          : 'Provide a detailed summary covering all key points and decisions.';

      console.log(`[SUMMARIZE] Formatted ${messages.length} messages for model`);
      
      const prompt = new ChatPrompt({
        messages: [
          {
            role: 'user',
            content: `Summarize the following chat conversation. ${lengthInstruction}\n\nChat:\n${formattedMessages}`,
          },
        ],
        instructions: 'You are a helpful assistant that summarizes conversations accurately and concisely.',
        model: this.model,
      });

      console.log(`[SUMMARIZE] Sending summarization request to model`);

      const response = await prompt.send('');
      console.log(`[SUMMARIZE] Model response received successfully`);
      return response.content || 'Failed to generate summary.';
    } catch (error) {
      console.error(`[SUMMARIZE_ERROR] Failed to summarize messages:`, error);
      return 'Error generating summary.';
    }
  }

  /**
   * Generate meeting minutes from chat and participants
   */
  async generateMeetingMinutes(
    messages: ChatMessage[],
    participants: string[],
    meetingTopic: string
  ): Promise<string> {
    try {
      console.log(`[MINUTES] Starting meeting minutes generation (${messages.length} messages, ${participants.length} participants)`);
      
      const formattedMessages = messages
        .reverse()
        .filter((msg) => msg.from != null && msg.body?.content && msg.body.content.trim().length > 0 && msg.messageType !== 'systemEventMessage')
        .map((msg) => `${msg.from?.user?.displayName || 'Unknown'}: ${msg.body.content.replace(/<[^>]*>/g, '').trim()}`)
        .filter((line) => line.length > 10)
        .join('\n');

      console.log(`[MINUTES] Formatted messages and prepared participants list`);

      const prompt = new ChatPrompt({
        messages: [
          {
            role: 'user',
            content: `Generate meeting minutes for the following:
Topic: ${meetingTopic}
Participants: ${participants.join(', ')}

Chat conversation:
${formattedMessages}

Format the minutes with sections for: Overview, Key Decisions, Action Items, and Next Steps.`,
          },
        ],
        instructions:
          'You are an expert at creating professional meeting minutes. Be concise but comprehensive.',
        model: this.model,
      });

      const response = await prompt.send('');
      console.log(`[MINUTES] Meeting minutes generated successfully`);
      return response.content || 'Failed to generate meeting minutes.';
    } catch (error) {
      console.error(`[MINUTES_ERROR] Failed to generate meeting minutes:`, error);
      return 'Error generating meeting minutes.';
    }
  }

  /**
   * Extract action items from chat messages
   */
  async extractActionItems(messages: ChatMessage[]): Promise<string[]> {
    try {
      console.log(`[ACTION_ITEMS] Starting extraction from ${messages.length} messages`);
      
      const formattedMessages = messages
        .reverse()
        .filter((msg) => msg.from != null && msg.body?.content && msg.body.content.trim().length > 0 && msg.messageType !== 'systemEventMessage')
        .map((msg) => `${msg.from?.user?.displayName || 'Unknown'}: ${msg.body.content.replace(/<[^>]*>/g, '').trim()}`)
        .filter((line) => line.length > 10)
        .join('\n');

      const prompt = new ChatPrompt({
        messages: [
          {
            role: 'user',
            content: `Extract all action items mentioned in this chat conversation. List only the action items, one per line.

Chat:
${formattedMessages}`,
          },
        ],
        instructions: 'Extract clear, actionable items from the conversation.',
        model: this.model,
      });

      console.log(`[ACTION_ITEMS] Sending action items extraction request to model`);
      const response = await prompt.send('');
      const actionItems = response.content
        ?.split('\n')
        .filter((item) => item.trim().length > 0)
        .map((item) => item.replace(/^[-•*]\s/, '').trim()) || [];

      console.log(`[ACTION_ITEMS] Extracted ${actionItems.length} action items`);
      return actionItems;
    } catch (error) {
      console.error(`[ACTION_ITEMS_ERROR] Failed to extract action items:`, error);
      return [];
    }
  }
}

export default new SummarizationHelper();
